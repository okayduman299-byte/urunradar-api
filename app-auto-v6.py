from __future__ import annotations

import asyncio
import html as html_lib
import json
import logging
import os
import re
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urlparse

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from playwright.async_api import BrowserContext, TimeoutError as PlaywrightTimeoutError, async_playwright

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("URUNRADAR_DB", BASE_DIR / "urunradar.db"))
COLLECT_HOURS = max(int(os.getenv("COLLECT_HOURS", "6")), 1)
DISCOVERY_HOURS = max(int(os.getenv("DISCOVERY_HOURS", "12")), 1)
DISCOVERY_LIMIT_PER_KEYWORD = max(int(os.getenv("DISCOVERY_LIMIT_PER_KEYWORD", "2")), 1)
MAX_AUTO_PRODUCTS = max(int(os.getenv("MAX_AUTO_PRODUCTS", "40")), 5)
DISCOVERY_KEYWORDS = [
    x.strip()
    for x in os.getenv(
        "DISCOVERY_KEYWORDS",
        "airfryer,robot süpürge,bluetooth kulaklık,akıllı saat,powerbank,"
        "şarjlı dikey süpürge,kahve makinesi,oyuncu kulaklığı",
    ).split(",")
    if x.strip()
]

CATEGORY_SOURCES = [
    {"name": "Airfryer", "url": "https://www.trendyol.com/fritoz-x-c1055"},
    {"name": "Robot Süpürge", "url": "https://www.trendyol.com/robot-supurge-x-c109453"},
    {"name": "Bluetooth Kulaklık", "url": "https://www.trendyol.com/bluetooth-kulaklik-x-c108626"},
    {"name": "Akıllı Saat", "url": "https://www.trendyol.com/akilli-saat-x-c1240"},
    {"name": "Powerbank", "url": "https://www.trendyol.com/tasinabilir-batarya-x-c103113"},
    {"name": "Şarjlı Dikey Süpürge", "url": "https://www.trendyol.com/sarjli-dik-supurge-x-c109454-a49-v1050428"},
    {"name": "Kahve Makinesi", "url": "https://www.trendyol.com/kahve-makinesi-x-c1079"},
]

USER_AGENT = os.getenv(
    "URUNRADAR_USER_AGENT",
    "Mozilla/5.0 (Linux; Android 13; Mobile) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0 Mobile Safari/537.36",
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
        raise ValueError("Yalnızca Trendyol ürün bağlantıları destekleniyor.")
    clean = f"https://www.trendyol.com{parsed.path}"
    return clean.rstrip("/")


async def resolve_trendyol_url(url: str) -> str:
    """Tam trendyol.com veya ty.gl kısa bağlantısını ürün URL'sine çevirir."""
    raw = url.strip()
    parsed = urlparse(raw)
    host = parsed.netloc.lower().replace("www.", "")

    if host in {"trendyol.com", "m.trendyol.com"}:
        return normalize_trendyol_url(raw)

    if host == "ty.gl":
        headers = {
            "User-Agent": USER_AGENT,
            "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.7",
        }
        timeout = httpx.Timeout(20.0, connect=10.0)
        async with httpx.AsyncClient(headers=headers, timeout=timeout, follow_redirects=True) as client:
            response = await client.get(raw)
            response.raise_for_status()
        return normalize_trendyol_url(str(response.url))

    raise ValueError("Trendyol veya ty.gl ürün bağlantısı kullan.")




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



def existing_product_count() -> int:
    with get_conn() as conn:
        return int(
            conn.execute("SELECT COUNT(*) FROM products WHERE is_active = 1").fetchone()[0]
        )


def product_exists(url: str) -> bool:
    with get_conn() as conn:
        return (
            conn.execute("SELECT 1 FROM products WHERE url = ? LIMIT 1", (url,)).fetchone()
            is not None
        )


def _candidate_url(raw_url: str) -> str | None:
    """Normalize a discovered Trendyol product URL."""
    if not raw_url:
        return None

    value = html_lib.unescape(str(raw_url))
    value = value.replace("\\/", "/").replace("\\u002F", "/")
    value = value.strip().strip('"').strip("'")

    if value.startswith("//"):
        value = "https:" + value

    if value.startswith("http"):
        parsed = urlparse(value)
        host = parsed.netloc.lower().replace("www.", "")
        if host not in {"trendyol.com", "m.trendyol.com"}:
            return None
        path = parsed.path
    else:
        path = value.split("?", 1)[0]
        if not path.startswith("/"):
            path = "/" + path

    if not re.search(r"-p-\d+", path, re.IGNORECASE):
        return None

    return f"https://www.trendyol.com{path}".rstrip("/")


def extract_product_candidates(
    html: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    Trendyol arama/kategori HTML'inden kamuya açık ürün bağlantılarını çıkarır.

    İlk yöntem normal <a href> bağlantılarıdır.
    Arama sayfası ürünleri gömülü JSON/JS içinde taşıyorsa ham HTML regex
    taraması yedek yöntem olarak kullanılır.
    """
    soup = BeautifulSoup(html, "html.parser")
    seen: set[str] = set()
    candidates: list[dict[str, Any]] = []

    def add_candidate(raw_url: str, context_text: str = "") -> None:
        if len(candidates) >= limit:
            return

        url = _candidate_url(raw_url)
        if not url or url in seen:
            return

        seen.add(url)

        badge_rank = None
        badge_type = None
        patterns = [
            ("çok_satan", r"En Çok Satan\s*(\d+)\.\s*Ürün"),
            ("çok_ziyaret", r"En Çok Ziyaret Edilen\s*(\d+)\.\s*Ürün"),
            ("çok_favori", r"En Çok Favorilenen\s*(\d+)\.\s*Ürün"),
            ("çok_değerlendirilen", r"En Çok Değerlendirilen\s*(\d+)\.\s*Ürün"),
        ]
        for kind, pattern in patterns:
            match = re.search(pattern, context_text, re.IGNORECASE)
            if match:
                badge_type = kind
                badge_rank = int(match.group(1))
                break

        candidates.append(
            {
                "url": url,
                "page_position": len(candidates) + 1,
                "badge_type": badge_type,
                "badge_rank": badge_rank,
            }
        )

    # 1) Normal linkler.
    for anchor in soup.find_all("a", href=True):
        context_text = ""
        node = anchor
        for _ in range(4):
            node = node.parent
            if node is None:
                break
            text = node.get_text(" ", strip=True)
            if len(text) > len(context_text):
                context_text = text
            if len(context_text) > 500:
                break

        add_candidate(str(anchor.get("href") or ""), context_text)
        if len(candidates) >= limit:
            return candidates


    # 2) JSON-LD ItemList / URL alanları.
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = tag.string or tag.get_text() or ""
        if not text:
            continue
        try:
            payload = json.loads(text)
        except Exception:
            continue

        stack = payload if isinstance(payload, list) else [payload]
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                for key, value in item.items():
                    if key in {"url", "item", "@id"} and isinstance(value, str):
                        add_candidate(value)
                    elif isinstance(value, (dict, list)):
                        stack.append(value)
            elif isinstance(item, list):
                stack.extend(item)

        if len(candidates) >= limit:
            return candidates

    # 3) Ham href taraması.
    raw_href_patterns = [
        r'href=["\\\']([^"\\\']*-p-\\d+[^"\\\']*)["\\\']',
        r'(?:https?:)?//(?:www\\.)?trendyol\\.com/[^"\\\'<>\\s]+-p-\\d+[^"\\\'<>\\s]*',
    ]
    for pattern in raw_href_patterns:
        for match in re.finditer(pattern, html, re.IGNORECASE):
            raw = match.group(1) if match.lastindex else match.group(0)
            add_candidate(raw)
            if len(candidates) >= limit:
                return candidates

    # 4) Gömülü JSON/JS içindeki URL alanları.
    field_patterns = [
        r'(?:"(?:url|href|webUrl|productUrl|product_url)"\s*:\s*")([^"]*-p-\d+[^"]*)"',
        r"(?:https?:)?\\?/\\?/(?:www\\?\.)?trendyol\\?\.com\\?/[^\"'<>\\s]+?-p-\d+[^\"'<>\\s]*",
        r'(?:"|\\")(/[^"\\]+?-p-\d+[^"\\]*)',
    ]

    for pattern in field_patterns:
        for match in re.finditer(pattern, html, re.IGNORECASE):
            raw = match.group(1) if match.lastindex else match.group(0)
            add_candidate(raw)
            if len(candidates) >= limit:
                return candidates

    return candidates


def candidate_priority(candidate: dict[str, Any]) -> tuple[int, int, int]:
    """
    Prefer public ranking badges over ordinary page position.
    Lower tuple sorts first.
    """
    badge_order = {
        "çok_satan": 0,
        "çok_ziyaret": 1,
        "çok_favori": 2,
        "çok_değerlendirilen": 3,
        None: 9,
    }
    badge_rank = int(candidate.get("badge_rank") or 9999)
    page_position = int(candidate.get("page_position") or 9999)
    return (
        badge_order.get(candidate.get("badge_type"), 8),
        badge_rank,
        page_position,
    )


async def _route_lightweight(route):
    """Block heavy assets while keeping scripts/XHR needed for product cards."""
    if route.request.resource_type in {"image", "media", "font"}:
        await route.abort()
    else:
        await route.continue_()


async def fetch_category_candidates(
    source: dict[str, str],
    context: BrowserContext,
    limit: int = 80,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Open Trendyol category pages in a real Chromium browser so client-side
    product cards can render, then collect public product links.
    """
    all_candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    page_attempts: list[dict[str, Any]] = []
    page_numbers = [2]

    page = await context.new_page()
    try:
        for page_no in page_numbers:
            separator = "&" if "?" in source["url"] else "?"
            page_url = (
                source["url"]
                if page_no == 1
                else f'{source["url"]}{separator}pi={page_no}'
            )

            try:
                response = await page.goto(
                    page_url,
                    wait_until="domcontentloaded",
                    timeout=20_000,
                )

                # Product cards are hydrated after the first HTML response.
                try:
                    await page.wait_for_selector(
                        'a[href*="-p-"]',
                        timeout=8_000,
                    )
                except PlaywrightTimeoutError:
                    await page.wait_for_timeout(1_500)

                rendered = await page.locator('a[href*="-p-"]').evaluate_all(
                    """
                    els => els.map((a, index) => {
                      let node = a;
                      let text = '';
                      for (let i = 0; i < 5 && node; i++, node = node.parentElement) {
                        const current = (node.innerText || '').trim();
                        if (current.length > text.length) text = current;
                        if (text.length > 700) break;
                      }
                      return {
                        href: a.href || a.getAttribute('href') || '',
                        text: text.slice(0, 900),
                        index
                      };
                    })
                    """
                )

                page_candidates: list[dict[str, Any]] = []
                for item in rendered:
                    url = _candidate_url(str(item.get("href") or ""))
                    if not url:
                        continue

                    context_text = str(item.get("text") or "")
                    badge_rank = None
                    badge_type = None
                    patterns = [
                        ("çok_satan", r"En Çok Satan\s*(\d+)\.\s*Ürün"),
                        ("çok_ziyaret", r"En Çok Ziyaret Edilen\s*(\d+)\.\s*Ürün"),
                        ("çok_favori", r"En Çok Favorilenen\s*(\d+)\.\s*Ürün"),
                        ("çok_değerlendirilen", r"En Çok Değerlendirilen\s*(\d+)\.\s*Ürün"),
                    ]
                    for kind, pattern in patterns:
                        match = re.search(pattern, context_text, re.IGNORECASE)
                        if match:
                            badge_type = kind
                            badge_rank = int(match.group(1))
                            break

                    page_candidates.append(
                        {
                            "url": url,
                            "page_position": int(item.get("index", 0)) + 1,
                            "badge_type": badge_type,
                            "badge_rank": badge_rank,
                            "source_page": page_no,
                        }
                    )

                page_attempts.append(
                    {
                        "page": page_no,
                        "http_status": response.status if response else None,
                        "title": (await page.title())[:160],
                        "rendered_links": len(rendered),
                        "candidates": len(page_candidates),
                        "final_url": page.url,
                    }
                )

                for candidate in page_candidates:
                    url = candidate["url"]
                    if url in seen:
                        continue
                    seen.add(url)
                    all_candidates.append(candidate)
                    if len(all_candidates) >= limit:
                        break

                if len(all_candidates) >= limit:
                    break

            except Exception as exc:
                page_attempts.append(
                    {
                        "page": page_no,
                        "error": str(exc)[:240],
                        "candidates": 0,
                    }
                )

        all_candidates.sort(key=candidate_priority)
        return all_candidates[:limit], {
            "attempts": page_attempts,
            "total_candidates": len(all_candidates),
        }
    finally:
        await page.close()


async def discover_category(
    source: dict[str, str],
    max_new: int,
    context: BrowserContext,
) -> dict[str, Any]:
    candidates, page_info = await fetch_category_candidates(
        source,
        context,
        limit=max(max_new * 30, 80),
    )

    added = 0
    failed = 0
    checked = 0
    errors: list[str] = []

    for candidate in candidates:
        if added >= max_new or existing_product_count() >= MAX_AUTO_PRODUCTS:
            break

        url = candidate["url"]
        if product_exists(url):
            continue

        checked += 1
        try:
            data = await fetch_public_product(url)
            data["category"] = source["name"]
            data["rank_position"] = (
                candidate.get("badge_rank")
                or candidate.get("page_position")
            )
            data["source"] = "browser_category_discovery"
            data["discovery_badge"] = candidate.get("badge_type")
            save_product_and_snapshot(data)
            added += 1
        except Exception as exc:
            failed += 1
            if len(errors) < 3:
                errors.append(str(exc)[:180])
            logger.info("Browser discovery skip %s: %s", url, exc)

    return {
        "category": source["name"],
        "url": source["url"],
        "source": "rendered_category_page",
        "candidates": len(candidates),
        "added": added,
        "failed": failed,
        "checked": checked,
        "page": page_info,
        "errors": errors,
    }


async def discover_all() -> dict[str, Any]:
    """
    Fast bounded discovery:
    - scans four public categories concurrently
    - one rendered page per category
    - fetches a small number of product details concurrently
    - returns within a bounded time instead of leaving the UI on "Taranıyor..."
    """
    before = existing_product_count()
    sources = CATEGORY_SOURCES[:4]
    category_results: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
        )
        context = await browser.new_context(
            locale="tr-TR",
            timezone_id="Europe/Istanbul",
            viewport={"width": 1365, "height": 900},
        )
        await context.route("**/*", _route_lightweight)

        try:
            scan_tasks = [
                fetch_category_candidates(source, context, limit=20)
                for source in sources
            ]
            try:
                scan_outputs = await asyncio.wait_for(
                    asyncio.gather(*scan_tasks, return_exceptions=True),
                    timeout=35,
                )
            except asyncio.TimeoutError:
                scan_outputs = [TimeoutError("Kategori taraması 35 saniyeyi aştı.")] * len(sources)

            candidates_to_fetch: list[tuple[dict[str, Any], dict[str, str]]] = []

            for source, output in zip(sources, scan_outputs):
                if isinstance(output, Exception):
                    category_results.append(
                        {
                            "category": source["name"],
                            "url": source["url"],
                            "source": "rendered_category_page",
                            "candidates": 0,
                            "added": 0,
                            "failed": 1,
                            "checked": 0,
                            "page": {},
                            "errors": [str(output)[:180]],
                        }
                    )
                    continue

                candidates, page_info = output
                category_results.append(
                    {
                        "category": source["name"],
                        "url": source["url"],
                        "source": "rendered_category_page",
                        "candidates": len(candidates),
                        "added": 0,
                        "failed": 0,
                        "checked": 0,
                        "page": page_info,
                        "errors": [],
                    }
                )

                # One fresh product per category keeps the request fast.
                for candidate in candidates:
                    if not product_exists(candidate["url"]):
                        candidates_to_fetch.append((candidate, source))
                        break

            # Cap detail fetches and run them concurrently.
            candidates_to_fetch = candidates_to_fetch[:4]

            async def fetch_one(
                candidate: dict[str, Any],
                source: dict[str, str],
            ) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
                data = await fetch_public_product(candidate["url"])
                return data, candidate, source

            detail_tasks = [
                fetch_one(candidate, source)
                for candidate, source in candidates_to_fetch
            ]

            try:
                detail_outputs = await asyncio.wait_for(
                    asyncio.gather(*detail_tasks, return_exceptions=True),
                    timeout=30,
                )
            except asyncio.TimeoutError:
                detail_outputs = [TimeoutError("Ürün detayları 30 saniyeyi aştı.")] * len(detail_tasks)

            total_added = 0
            total_failed = 0

            for item, pair in zip(detail_outputs, candidates_to_fetch):
                candidate, source = pair
                result_row = next(
                    row for row in category_results
                    if row["category"] == source["name"]
                )
                result_row["checked"] += 1

                if isinstance(item, Exception):
                    result_row["failed"] += 1
                    total_failed += 1
                    if len(result_row["errors"]) < 3:
                        result_row["errors"].append(str(item)[:180])
                    continue

                data, candidate, source = item
                data["category"] = source["name"]
                data["rank_position"] = (
                    candidate.get("badge_rank")
                    or candidate.get("page_position")
                )
                data["source"] = "browser_category_discovery"
                data["discovery_badge"] = candidate.get("badge_type")

                try:
                    save_product_and_snapshot(data)
                    result_row["added"] += 1
                    total_added += 1
                except Exception as exc:
                    result_row["failed"] += 1
                    total_failed += 1
                    if len(result_row["errors"]) < 3:
                        result_row["errors"].append(str(exc)[:180])

        finally:
            await context.close()
            await browser.close()

    total_candidates = sum(
        int(row.get("candidates", 0))
        for row in category_results
    )

    return {
        "ok": True,
        "engine": "browser_render_v6_fast",
        "before": before,
        "after": existing_product_count(),
        "added": total_added,
        "failed": total_failed,
        "candidates": total_candidates,
        "categories": category_results,
        "notice": (
            "Hızlı tarama dört kategoriyi eşzamanlı kontrol eder ve "
            "yaklaşık bir dakika içinde sonuç döndürür."
        ),
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
        scheduler.add_job(
            discover_all,
            "interval",
            hours=DISCOVERY_HOURS,
            id="discover_all",
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
        "discovery_hours": DISCOVERY_HOURS,
        "auto_keywords": DISCOVERY_KEYWORDS,
        "discovery_engine": "browser_render_v6_fast",
        "category_sources": CATEGORY_SOURCES,
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
        url = await resolve_trendyol_url(request.url)
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


@app.post("/api/discovery/run")
async def discovery_now():
    return await discover_all()


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


MOBILE_HTML = r"""
<!doctype html>
<html lang="tr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#f6f8fc">
<title>ÜrünRadar Canlı</title>
<style>
:root{--bg:#f6f8fc;--surface:#fff;--text:#111525;--muted:#788096;--line:#e7eaf2;--blue:#2d6ef7;--green:#12b776;--purple:#764df6;--red:#e34850;--shadow:0 12px 30px rgba(31,45,84,.08)}
*{box-sizing:border-box}body{margin:0;background:var(--bg);font-family:Inter,system-ui,-apple-system,"Segoe UI",sans-serif;color:var(--text)}
button,input{font:inherit}.app{max-width:760px;margin:auto;padding-bottom:92px}.top{position:sticky;top:0;z-index:10;background:rgba(246,248,252,.95);backdrop-filter:blur(16px);padding:20px 18px 12px;display:flex;align-items:center;justify-content:space-between}
.brand{font-size:30px;font-weight:950;letter-spacing:-1.4px}.brand span{color:var(--blue)}.sub{color:var(--muted);font-size:13px}.status{padding:8px 11px;border-radius:999px;font-size:12px;font-weight:850;background:#fff;border:1px solid var(--line)}.status.ok{color:var(--green)}.status.err{color:var(--red)}
main,.page{padding:0 18px}.metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-top:12px}.metric{background:#fff;border:1px solid var(--line);border-radius:19px;padding:13px;box-shadow:var(--shadow);min-width:0}.metric small{display:block;color:var(--muted);min-height:34px}.metric strong{display:block;font-size:18px;margin-top:6px;overflow:hidden;text-overflow:ellipsis}.blue{color:var(--blue)}.green{color:var(--green)}
.notice{text-align:center;color:var(--muted);font-size:12px;margin:12px 0 20px}.head{display:flex;align-items:center;justify-content:space-between;margin:20px 0 10px}.head h2{margin:0;font-size:22px}.link{border:0;background:none;color:var(--blue);font-weight:850}
.stack{display:flex;flex-direction:column;gap:10px}.card{background:#fff;border:1px solid var(--line);border-radius:19px;padding:13px;box-shadow:var(--shadow);display:grid;grid-template-columns:42px minmax(0,1fr) auto;gap:10px;align-items:center}.rank{width:38px;height:38px;border-radius:50%;display:grid;place-items:center;background:#eef1f6;font-weight:900}.rank.gold{background:#ffbd20;color:#fff}
.name{font-weight:900;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.meta{font-size:12px;color:var(--muted);margin:4px 0}.stats{text-align:right;font-size:11px;color:var(--muted)}.stats strong{display:block;color:var(--text);font-size:15px;margin:2px 0 5px}.conf{color:var(--green);font-weight:900}.err{color:var(--red);font-size:11px;margin-top:4px}
.opp{background:linear-gradient(135deg,#fff,#f5f0ff);border:1px solid #dfd4ff;border-radius:20px;padding:15px;box-shadow:var(--shadow)}.opp-score{font-size:31px;font-weight:950;color:var(--purple)}.opp-score span{font-size:16px}.empty{background:#fff;border:1px dashed #cdd3df;border-radius:20px;padding:24px;text-align:center;color:var(--muted)}
.hidden{display:none!important}.page-head{display:flex;gap:10px;align-items:center;margin:10px 0 18px}.back{border:1px solid var(--line);background:#fff;width:42px;height:42px;border-radius:50%;font-size:25px}.page-head h1{margin:0}.page-head p{margin:3px 0 0;color:var(--muted);font-size:13px}
.form-card{background:#fff;border:1px solid var(--line);border-radius:20px;padding:16px;box-shadow:var(--shadow)}label{display:block;font-size:12px;color:var(--muted);font-weight:800;margin-bottom:6px}input{width:100%;border:1px solid var(--line);border-radius:14px;padding:13px;outline:none;margin-bottom:12px}.btn{border:0;border-radius:14px;padding:12px 14px;font-weight:900}.primary{background:var(--blue);color:#fff}.dark{background:#111525;color:#fff}.full{width:100%}
.nav{position:fixed;bottom:0;left:50%;transform:translateX(-50%);width:min(760px,100%);height:76px;background:rgba(255,255,255,.97);backdrop-filter:blur(15px);border-top:1px solid var(--line);display:grid;grid-template-columns:repeat(3,1fr);z-index:20}.nav button{border:0;background:none;color:#8a91a3;font-size:22px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:3px}.nav span{font-size:11px;font-weight:800}.nav .active{color:var(--blue)}
.toast{position:fixed;bottom:95px;left:50%;transform:translateX(-50%) translateY(20px);opacity:0;background:#111525;color:#fff;padding:11px 15px;border-radius:999px;transition:.25s;z-index:30;font-size:12px}.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
@media(max-width:520px){.metric{padding:10px}.metric strong{font-size:15px}.card{grid-template-columns:38px minmax(0,1fr)}.stats{grid-column:2;display:grid;grid-template-columns:1fr 1fr;text-align:left;background:#f7f8fb;border-radius:12px;padding:8px}}
</style>
</head>
<body>
<div class="app">
<header class="top">
  <div><div class="brand">Ürün<span>Radar</span></div><div class="sub">Trendyol Otomatik Radar v6</div></div>
  <div id="serverStatus" class="status">Kontrol…</div>
</header>

<main id="homePage">
  <section class="metrics">
    <article class="metric"><small>Tahmini Toplam Satış</small><strong class="blue" id="totalSales">—</strong></article>
    <article class="metric"><small>Tahmini Toplam Ciro</small><strong class="green" id="totalRevenue">—</strong></article>
    <article class="metric"><small>Trend Ürün</small><strong id="trendProduct">—</strong></article>
  </section>
  <div class="notice">* Rakip ürün satış ve ciro değerleri tahmindir.</div>
  <section><div class="head"><h2>En Çok Satanlar</h2><button class="link" data-route="products">Tümünü Gör ›</button></div><div id="topProducts" class="stack"></div></section>
  <section><div class="head"><h2>Fırsat Ürünü</h2></div><div id="opportunity"></div></section>
</main>

<section id="productsPage" class="page hidden">
  <div class="page-head"><button class="back" data-route="home">‹</button><div><h1>Ürünler</h1><p>Canlı sunucudaki izleme listesi</p></div></div>
  <div id="allProducts" class="stack"></div>
</section>

<section id="addPage" class="page hidden">
  <div class="page-head"><button class="back" data-route="home">‹</button><div><h1>Otomatik Keşif</h1><p>Sistem ürünleri kendisi bulup izlemeye alır</p></div></div>
  <div class="form-card">
    <label>Otomatik ürün tarama</label>
    <button id="discoverBtn" class="btn primary full">Şimdi Otomatik Tara</button>
    <p class="meta">Popüler arama alanlarından yeni ürün adaylarını bulur, tekrarları atlar ve izlemeye ekler.</p>
  </div>
  <div class="head"><h2>İsteğe bağlı manuel ekleme</h2></div>
  <div class="form-card">
    <label>Trendyol ürün bağlantısı</label>
    <input id="productUrl" type="url" placeholder="https://www.trendyol.com/...">
    <button id="watchBtn" class="btn primary full">Ürünü İzlemeye Başla</button>
    <p class="meta">İlk gözlem hemen alınır. Sonraki gözlemler sunucu tarafından periyodik toplanır.</p>
  </div>
  <div class="head"><h2>Toplama</h2></div>
  <button id="collectBtn" class="btn dark full">Şimdi Tüm Ürünleri Güncelle</button>
</section>

<nav class="nav">
  <button class="active" data-route="home">⌂<span>Ana Sayfa</span></button>
  <button data-route="products">▢<span>Ürünler</span></button>
  <button data-route="add">✦<span>Otomatik Tara</span></button>
</nav>
<div id="toast" class="toast"></div>
</div>

<script>
const state={products:[],route:"home"};
const api=path=>path;
function fmt(n){return new Intl.NumberFormat("tr-TR").format(Math.round(Number(n)||0))}
function money(n){return fmt(n)+" TL"}
function esc(s=""){return String(s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]))}
function toast(msg){const e=document.getElementById("toast");e.textContent=msg;e.classList.add("show");setTimeout(()=>e.classList.remove("show"),2600)}
async function jsonFetch(path,options={}){
  const controller=new AbortController();
  const timer=setTimeout(()=>controller.abort(),90000);
  try{
    const res=await fetch(api(path),{
      headers:{"Content-Type":"application/json",...(options.headers||{})},
      signal:controller.signal,
      ...options
    });
    let data={};try{data=await res.json()}catch{}
    if(!res.ok)throw new Error(data.detail||("HTTP "+res.status));
    return data;
  }catch(e){
    if(e.name==="AbortError")throw new Error("Tarama 90 saniyeyi aştı ve durduruldu.");
    throw e;
  }finally{
    clearTimeout(timer);
  }
}
function card(p,i){return `<article class="card"><div class="rank ${i===0?"gold":""}">${i+1}</div><div><div class="name">${esc(p.name)}</div><div class="meta">${esc(p.platform)} · ${p.snapshot_count||0} gözlem · Yorum Δ ${fmt(p.review_delta||0)}</div><div class="conf">Güven %${fmt(p.confidence)}</div>${p.last_error?`<div class="err">${esc(p.last_error)}</div>`:""}</div><div class="stats"><div>Tahmini Satış<strong>${fmt(p.estimated_sales)} adet</strong></div><div>Tahmini Ciro<strong>${money(p.estimated_revenue)}</strong></div></div></article>`}
function render(){
  const list=state.products.slice().sort((a,b)=>b.estimated_sales-a.estimated_sales);
  const sales=list.reduce((s,p)=>s+(p.estimated_sales||0),0);
  const revenue=list.reduce((s,p)=>s+(p.estimated_revenue||0),0);
  document.getElementById("totalSales").textContent=fmt(sales)+" adet";
  document.getElementById("totalRevenue").textContent=money(revenue);
  document.getElementById("trendProduct").textContent=list[0]?.name||"—";
  document.getElementById("topProducts").innerHTML=list.length?list.slice(0,4).map(card).join(""):`<div class="empty">Henüz ürün yok. Sistem otomatik keşif yapacak.</div>`;
  document.getElementById("allProducts").innerHTML=list.length?list.map(card).join(""):`<div class="empty">İzlenen ürün yok.</div>`;
  const opp=list.slice().sort((a,b)=>b.opportunity_score-a.opportunity_score)[0];
  document.getElementById("opportunity").innerHTML=opp?`<article class="opp"><div class="name">${esc(opp.name)}</div><div class="meta">${esc(opp.platform)} · ${fmt(opp.seller_count)} satıcı sinyali</div><div class="opp-score">${opp.opportunity_score}<span>/100</span></div><div class="meta">Tahmini ciro ${money(opp.estimated_revenue)} · Güven %${opp.confidence}</div></article>`:`<div class="empty">Fırsat analizi için ürün ekle.</div>`;
}
async function load(){
  const status=document.getElementById("serverStatus");status.className="status";status.textContent="Kontrol…";
  try{
    await jsonFetch("/api/health");
    state.products=await jsonFetch("/api/products");
    status.className="status ok";status.textContent="● Canlı";
    render();
    if(state.products.length===0){
      status.textContent="● Otomatik tarama";
      try{
        await jsonFetch("/api/discovery/run",{method:"POST"});
        state.products=await jsonFetch("/api/products");
      }catch(_e){}
      status.textContent="● Canlı";
      render();
    }
  }catch(e){
    status.className="status err";status.textContent="● Bağlı değil";
    state.products=[];render();
  }
}
function route(name){
  state.route=name;
  ["home","products","add"].forEach(x=>{
    const id=x==="home"?"homePage":x+"Page";
    document.getElementById(id).classList.toggle("hidden",x!==name);
  });
  document.querySelectorAll(".nav button").forEach(b=>b.classList.toggle("active",b.dataset.route===name));
  window.scrollTo({top:0,behavior:"smooth"});
}
document.addEventListener("click",e=>{const r=e.target.closest("[data-route]")?.dataset.route;if(r)route(r)});
document.getElementById("discoverBtn").onclick=async()=>{
  const btn=document.getElementById("discoverBtn");btn.disabled=true;btn.textContent="Taranıyor…";
  try{
    const r=await jsonFetch("/api/discovery/run",{method:"POST"});
    const firstError = (r.categories || [])
      .flatMap(x => x.errors || [])
      .find(Boolean);
    const firstAttempt = (r.categories || [])
      .flatMap(x => (x.page && x.page.attempts) || [])
      .find(Boolean);
    const diagnostic = firstAttempt
      ? `Sayfa ${firstAttempt.page}: HTTP ${firstAttempt.http_status || "?"}, ${firstAttempt.rendered_links || 0} link, ${firstAttempt.candidates || 0} aday.`
      : "";
    toast(
      r.added > 0
        ? `${r.added} yeni ürün eklendi. ${r.candidates || 0} aday tarandı.`
        : firstError
          ? `0 ürün. Hata: ${firstError}`
          : `0 ürün. ${diagnostic}`
    );
    await load();route("products");
  }catch(e){toast(e.message)}
  finally{btn.disabled=false;btn.textContent="Şimdi Otomatik Tara"}
};
document.getElementById("watchBtn").onclick=async()=>{
  const url=document.getElementById("productUrl").value.trim();
  if(!url){toast("Trendyol ürün bağlantısını yapıştır.");return}
  const btn=document.getElementById("watchBtn");btn.disabled=true;btn.textContent="Ekleniyor…";
  try{
    await jsonFetch("/api/watch",{method:"POST",body:JSON.stringify({url})});
    document.getElementById("productUrl").value="";
    toast("Ürün izlemeye alındı.");
    await load();route("products");
  }catch(e){toast(e.message)}
  finally{btn.disabled=false;btn.textContent="Ürünü İzlemeye Başla"}
};
document.getElementById("collectBtn").onclick=async()=>{
  const btn=document.getElementById("collectBtn");btn.disabled=true;btn.textContent="Güncelleniyor…";
  try{
    const r=await jsonFetch("/api/collect/run",{method:"POST"});
    toast(`${r.success} ürün güncellendi, ${r.failed} hata.`);
    await load();
  }catch(e){toast(e.message)}
  finally{btn.disabled=false;btn.textContent="Şimdi Tüm Ürünleri Güncelle"}
};
load();
</script>
</body>
</html>
"""

@app.get("/mobile", response_class=HTMLResponse)
def mobile_app():
    return HTMLResponse(MOBILE_HTML)
