from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("URUNRADAR_DB", BASE_DIR / "urunradar.db"))
COLLECT_HOURS = max(int(os.getenv("COLLECT_HOURS", "6")), 1)
USER_AGENT = os.getenv(
    "URUNRADAR_USER_AGENT",
    "UrunRadarPilot/1.0 (+product-market-research; contact: configure-your-email)",
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("urunradar")

scheduler = AsyncIOScheduler(timezone="Europe/Istanbul")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                url TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                category TEXT DEFAULT 'Genel',
                image_url TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                last_error TEXT
            );

            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                observed_at TEXT NOT NULL,
                price REAL,
                rating REAL,
                review_count INTEGER,
                seller_count INTEGER,
                availability TEXT,
                rank_position INTEGER,
                source TEXT NOT NULL DEFAULT 'public_page',
                raw_json TEXT,
                FOREIGN KEY(product_id) REFERENCES products(id)
            );

            CREATE INDEX IF NOT EXISTS idx_snapshots_product_time
            ON snapshots(product_id, observed_at DESC);
            """
        )
        conn.commit()


def normalize_trendyol_url(url: str) -> str:
    parsed = urlparse(url.strip())
    host = parsed.netloc.lower().replace("www.", "")
    if host not in {"trendyol.com", "m.trendyol.com"}:
        raise ValueError("Şimdilik yalnızca trendyol.com ürün bağlantıları destekleniyor.")
    clean = f"https://www.trendyol.com{parsed.path}"
    return clean.rstrip("/")


def jsonld_objects(soup: BeautifulSoup) -> list[dict[str, Any]]:
    objects: list[dict[str, Any]] = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = tag.string or tag.get_text()
        if not text:
            continue
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            continue

        items = data if isinstance(data, list) else [data]
        for item in items:
            if isinstance(item, dict) and "@graph" in item and isinstance(item["@graph"], list):
                items.extend(x for x in item["@graph"] if isinstance(x, dict))
            elif isinstance(item, dict):
                objects.append(item)
        objects.extend(x for x in items if isinstance(x, dict) and x not in objects)
    return objects


def find_product_jsonld(objects: list[dict[str, Any]]) -> dict[str, Any] | None:
    for obj in objects:
        obj_type = obj.get("@type")
        if obj_type == "Product" or (isinstance(obj_type, list) and "Product" in obj_type):
            return obj
    return None


def first_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).replace(".", "").replace(",", ".")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    return float(match.group()) if match else None


def parse_public_product_page(html: str, url: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    product = find_product_jsonld(jsonld_objects(soup)) or {}

    name = product.get("name")
    if not name:
        title = soup.find("meta", attrs={"property": "og:title"})
        name = title.get("content") if title else None

    image_url = None
    image = product.get("image")
    if isinstance(image, list) and image:
        image_url = image[0]
    elif isinstance(image, str):
        image_url = image
    if not image_url:
        og_image = soup.find("meta", attrs={"property": "og:image"})
        image_url = og_image.get("content") if og_image else None

    offers = product.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    price = first_number(offers.get("price") or offers.get("lowPrice"))
    availability = offers.get("availability")
    if availability:
        availability = str(availability).split("/")[-1]

    aggregate = product.get("aggregateRating") or {}
    rating = first_number(aggregate.get("ratingValue"))
    review_count_num = first_number(aggregate.get("reviewCount") or aggregate.get("ratingCount"))
    review_count = int(review_count_num) if review_count_num is not None else None

    if not name:
        raise ValueError("Ürün adı sayfadan okunamadı. Sayfa yapısı değişmiş veya erişim kısıtlı olabilir.")

    return {
        "platform": "Trendyol",
        "url": url,
        "name": str(name).strip(),
        "category": "Genel",
        "image_url": image_url,
        "price": price,
        "rating": rating,
        "review_count": review_count,
        "seller_count": None,
        "availability": availability,
        "rank_position": None,
        "source": "public_page",
    }


async def fetch_public_product(url: str) -> dict[str, Any]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml",
    }
    timeout = httpx.Timeout(20.0, connect=10.0)
    async with httpx.AsyncClient(
        headers=headers,
        timeout=timeout,
        follow_redirects=True,
    ) as client:
        response = await client.get(url)
        response.raise_for_status()
    return parse_public_product_page(response.text, str(response.url))


def save_product_and_snapshot(data: dict[str, Any]) -> int:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM products WHERE url = ?",
            (data["url"],),
        ).fetchone()

        if row:
            product_id = int(row["id"])
            conn.execute(
                """
                UPDATE products
                SET name = ?, category = ?, image_url = ?, last_error = NULL
                WHERE id = ?
                """,
                (
                    data["name"],
                    data.get("category") or "Genel",
                    data.get("image_url"),
                    product_id,
                ),
            )
        else:
            cur = conn.execute(
                """
                INSERT INTO products (
                    platform, url, name, category, image_url, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    data.get("platform", "Trendyol"),
                    data["url"],
                    data["name"],
                    data.get("category") or "Genel",
                    data.get("image_url"),
                    now_iso(),
                ),
            )
            product_id = int(cur.lastrowid)

        conn.execute(
            """
            INSERT INTO snapshots (
                product_id, observed_at, price, rating, review_count,
                seller_count, availability, rank_position, source, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                product_id,
                now_iso(),
                data.get("price"),
                data.get("rating"),
                data.get("review_count"),
                data.get("seller_count"),
                data.get("availability"),
                data.get("rank_position"),
                data.get("source", "public_page"),
                json.dumps(data, ensure_ascii=False),
            ),
        )
        conn.commit()
        return product_id


def save_error(product_id: int, message: str) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE products SET last_error = ? WHERE id = ?",
            (message[:500], product_id),
        )
        conn.commit()


def snapshot_rows(product_id: int, limit: int = 30) -> list[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            """
            SELECT * FROM snapshots
            WHERE product_id = ?
            ORDER BY observed_at DESC
            LIMIT ?
            """,
            (product_id, limit),
        ).fetchall()


def estimate_metrics(product: sqlite3.Row, snapshots: list[sqlite3.Row]) -> dict[str, Any]:
    latest = snapshots[0] if snapshots else None
    previous = snapshots[1] if len(snapshots) > 1 else None

    latest_reviews = int(latest["review_count"] or 0) if latest else 0
    previous_reviews = int(previous["review_count"] or 0) if previous else 0
    review_delta = max(latest_reviews - previous_reviews, 0)

    rating = float(latest["rating"] or 0) if latest else 0
    price = float(latest["price"] or 0) if latest else 0
    rank = int(latest["rank_position"] or 100) if latest else 100
    seller_count = int(latest["seller_count"] or 1) if latest else 1
    available = bool(latest and latest["availability"] not in {"OutOfStock", "SoldOut"})

    # MVP heuristiği: sonuç gerçek satış değildir.
    rank_component = 2200 / (max(rank, 1) ** 0.55)
    review_component = review_delta * 5.2
    seller_component = min(max(seller_count, 1), 50) * 7.5
    availability_component = 120 if available else -120
    rating_component = max(rating - 4.0, 0) * 180
    estimated_sales = max(
        round(
            rank_component
            + review_component
            + seller_component
            + availability_component
            + rating_component
        ),
        0,
    )
    estimated_revenue = estimated_sales * price

    signals = 20  # price/page existence
    if latest and latest["review_count"] is not None:
        signals += 25
    if review_delta > 0:
        signals += 25
    if latest and latest["rating"] is not None:
        signals += 15
    if len(snapshots) >= 3:
        signals += 10
    confidence = min(signals, 90)

    demand = min(estimated_sales / 80, 45)
    low_competition = max(30 - seller_count * 0.7, 0)
    momentum = min(review_delta / 4, 20)
    margin_proxy = min(price / 1500, 5)
    opportunity = min(round(demand + low_competition + momentum + margin_proxy, 1), 100)

    return {
        "id": int(product["id"]),
        "platform": product["platform"],
        "url": product["url"],
        "name": product["name"],
        "category": product["category"],
        "image_url": product["image_url"],
        "price": price,
        "rating": rating or None,
        "review_count": latest_reviews or None,
        "review_delta": review_delta,
        "seller_count": seller_count,
        "availability": latest["availability"] if latest else None,
        "observed_at": latest["observed_at"] if latest else None,
        "estimated_sales": estimated_sales,
        "estimated_revenue": estimated_revenue,
        "confidence": confidence,
        "opportunity_score": opportunity,
        "last_error": product["last_error"],
        "snapshot_count": len(snapshots),
        "estimate_notice": "Rakip ürün satış ve ciro değerleri tahmindir.",
    }


def get_all_product_metrics() -> list[dict[str, Any]]:
    with get_conn() as conn:
        products = conn.execute(
            "SELECT * FROM products WHERE is_active = 1 ORDER BY id DESC"
        ).fetchall()

    result = []
    for product in products:
        result.append(estimate_metrics(product, snapshot_rows(int(product["id"]), 30)))
    return result


async def collect_product(product_id: int) -> dict[str, Any]:
    with get_conn() as conn:
        product = conn.execute(
            "SELECT * FROM products WHERE id = ? AND is_active = 1",
            (product_id,),
        ).fetchone()

    if not product:
        raise ValueError("Ürün bulunamadı.")

    try:
        data = await fetch_public_product(product["url"])
        save_product_and_snapshot(data)
        logger.info("Collected product %s", product_id)
        return data
    except Exception as exc:
        save_error(product_id, str(exc))
        logger.warning("Collect failed for product %s: %s", product_id, exc)
        raise


async def collect_all() -> dict[str, Any]:
    with get_conn() as conn:
        products = conn.execute(
            "SELECT id FROM products WHERE is_active = 1 ORDER BY id"
        ).fetchall()

    success = 0
    failed = 0
    for row in products:
        try:
            await collect_product(int(row["id"]))
            success += 1
        except Exception:
            failed += 1

    return {"success": success, "failed": failed, "total": success + failed}


class WatchRequest(BaseModel):
    url: str = Field(min_length=10)


class ManualSnapshotRequest(BaseModel):
    product_id: int
    price: float | None = None
    rating: float | None = Field(default=None, ge=0, le=5)
    review_count: int | None = Field(default=None, ge=0)
    seller_count: int | None = Field(default=None, ge=1)
    rank_position: int | None = Field(default=None, ge=1)
    availability: str | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    if not scheduler.running:
        scheduler.add_job(
            collect_all,
            "interval",
            hours=COLLECT_HOURS,
            id="collect_all",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        scheduler.start()
    yield
    if scheduler.running:
        scheduler.shutdown(wait=False)


app = FastAPI(
    title="ÜrünRadar Trendyol Pilot API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # MVP. Üretimde mobil alan adını sınırlandır.
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {
        "app": "ÜrünRadar Trendyol Pilot API",
        "status": "ok",
        "docs": "/docs",
        "notice": "Rakip ürün satış ve ciro değerleri tahmindir.",
    }


@app.get("/api/health")
def health():
    return {
        "ok": True,
        "time": now_iso(),
        "database": str(DB_PATH),
        "collect_hours": COLLECT_HOURS,
    }


@app.get("/api/products")
def products(
    platform: str | None = None,
    sort: str = Query(default="sales", pattern="^(sales|revenue|opportunity|recent)$"),
):
    items = get_all_product_metrics()
    if platform and platform.lower() != "tümü":
        items = [x for x in items if x["platform"].lower() == platform.lower()]

    keys = {
        "sales": "estimated_sales",
        "revenue": "estimated_revenue",
        "opportunity": "opportunity_score",
        "recent": "id",
    }
    return sorted(items, key=lambda x: x[keys[sort]], reverse=True)


@app.get("/api/products/{product_id}/trend")
def product_trend(product_id: int):
    rows = list(reversed(snapshot_rows(product_id, 60)))
    if not rows:
        raise HTTPException(status_code=404, detail="Ürün veya veri bulunamadı.")
    return [
        {
            "observed_at": row["observed_at"],
            "price": row["price"],
            "rating": row["rating"],
            "review_count": row["review_count"],
            "seller_count": row["seller_count"],
            "rank_position": row["rank_position"],
            "availability": row["availability"],
        }
        for row in rows
    ]


@app.post("/api/watch")
async def watch(request: WatchRequest):
    try:
        url = normalize_trendyol_url(request.url)
        data = await fetch_public_product(url)
        product_id = save_product_and_snapshot(data)
        return {
            "ok": True,
            "product_id": product_id,
            "product": data,
            "notice": "İlk gözlem kaydedildi. Tahmin zamanla daha anlamlı hale gelir.",
        }
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Ürün sayfasına erişilemedi: HTTP {exc.response.status_code}",
        ) from exc
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.post("/api/collect/run")
async def collect_now():
    return await collect_all()


@app.post("/api/manual-snapshot")
def manual_snapshot(request: ManualSnapshotRequest):
    with get_conn() as conn:
        product = conn.execute(
            "SELECT * FROM products WHERE id = ?",
            (request.product_id,),
        ).fetchone()
    if not product:
        raise HTTPException(status_code=404, detail="Ürün bulunamadı.")

    data = {
        "platform": product["platform"],
        "url": product["url"],
        "name": product["name"],
        "category": product["category"],
        "image_url": product["image_url"],
        "price": request.price,
        "rating": request.rating,
        "review_count": request.review_count,
        "seller_count": request.seller_count,
        "availability": request.availability,
        "rank_position": request.rank_position,
        "source": "manual",
    }
    product_id = save_product_and_snapshot(data)
    return {"ok": True, "product_id": product_id}


@app.get("/api/report/summary")
def report_summary():
    items = get_all_product_metrics()
    total_sales = sum(x["estimated_sales"] for x in items)
    total_revenue = sum(x["estimated_revenue"] for x in items)
    top = max(items, key=lambda x: x["estimated_sales"], default=None)
    opportunity = max(items, key=lambda x: x["opportunity_score"], default=None)
    return {
        "product_count": len(items),
        "estimated_total_sales": total_sales,
        "estimated_total_revenue": total_revenue,
        "top_product": top,
        "best_opportunity": opportunity,
        "notice": "Rakip ürün satış ve ciro değerleri tahmindir.",
    }
