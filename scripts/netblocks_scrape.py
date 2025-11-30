#!/usr/bin/env python3
"""
Scrape NetBlocks report pages for platform blocking incidents (robust).

Features:
- Retries/backoff + custom User-Agent
- Parses listing pages AND article detail pages
- Extracts title, url, published date, snippet, full text
- Matches platforms (from --keywords) and countries (from --countries)
- Dedupes by URL
- Polite randomized delay between requests
- Saves JSONL (streamable) and JSON (pretty) under output/

Example:
python scripts/netblocks_scrape.py \
  --pages 10 \
  --keywords "TikTok,Twitter,Facebook,Instagram,YouTube,WhatsApp,Telegram,Reddit,Snapchat,LinkedIn,WeChat,VK,Signal,Pinterest,Discord,Tumblr,Line,Medium,Viber,Threads" \
  --countries "IN,TR,RU,IR,SA,AE,EG,IQ,LB,ET,UG,SD,NG,KE,CN,PK,MM,TH,VN,SY,CU" \
  --timeout 120 --retries 5 --sleep 1.5 \
  --out_json output/netblocks_platform_cases.json \
  --out_jsonl output/netblocks_platform_cases.jsonl
"""

import argparse
import json
import os
import random
import re
import time
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry

LISTING_URL = "https://netblocks.org/reports/page/{page}"


def make_session(timeout: int, retries: int) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "User-Agent": "censorship-autocollect/1.0 (+https://github.com/klee283/censorship_autocollect)"
    })
    s.timeout = timeout
    return s


def get_soup(session: requests.Session, url: str, timeout: int) -> Optional[BeautifulSoup]:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def text_or_none(node) -> str:
    return node.get_text(" ", strip=True) if node else ""


def clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def compile_keyword_regex(keywords: List[str]) -> re.Pattern:
    parts = [re.escape(k) for k in keywords if k]
    return re.compile(r"\b(" + "|".join(parts) + r")\b", re.I) if parts else re.compile(r"$^")  # match nothing


def compile_country_regex(country_names: List[str]) -> re.Pattern:
    # Match whole words for country names (case-insensitive)
    parts = [re.escape(n) for n in country_names if n]
    return re.compile(r"\b(" + "|".join(parts) + r")\b", re.I) if parts else re.compile(r"$^")


def iso2_to_name_map(iso2_list: List[str]) -> Dict[str, str]:
    # Minimal internal map for your 21 focus countries (expand if needed)
    base = {
        "IN": "India", "TR": "Turkey", "RU": "Russia", "IR": "Iran", "SA": "Saudi Arabia",
        "AE": "United Arab Emirates", "EG": "Egypt", "IQ": "Iraq", "LB": "Lebanon", "ET": "Ethiopia",
        "UG": "Uganda", "SD": "Sudan", "NG": "Nigeria", "KE": "Kenya", "CN": "China",
        "PK": "Pakistan", "MM": "Myanmar", "TH": "Thailand", "VN": "Vietnam", "SY": "Syria", "CU": "Cuba"
    }
    out = {}
    for c in iso2_list:
        c = c.strip().upper()
        if not c:
            continue
        out[c] = base.get(c, c)  # fallback to code if name not in base
    return out


def parse_article_detail(session: requests.Session, url: str, timeout: int) -> Dict:
    soup = get_soup(session, url, timeout)
    if soup is None:
        return {}

    # Try common date locations on NetBlocks
    date = ""
    # <time datetime="...">
    t = soup.select_one("time[datetime]")
    if t and t.get("datetime"):
        date = t.get("datetime").strip()

    # OpenGraph publish time
    if not date:
        og = soup.select_one('meta[property="article:published_time"]')
        if og and og.get("content"):
            date = og["content"].strip()

    # Article content & tags (loose selectors to be robust)
    content_node = soup.select_one("article") or soup.select_one(".entry-content") or soup
    full_text = clean_spaces(content_node.get_text(" ", strip=True))[:8000]  # cap length

    # A short excerpt (to serve as "evidence" snippet)
    excerpt = full_text[:500]

    return {
        "published_at": date,
        "full_text": full_text,
        "excerpt": excerpt
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=5, help="How many listing pages to scan.")
    ap.add_argument("--keywords", type=str, default="Twitter,Facebook,TikTok,YouTube,Telegram,Instagram,WhatsApp,Signal,Snapchat,Reddit,LinkedIn,Discord,VK,WeChat,Tumblr,Line,Medium,Viber,Threads")
    ap.add_argument("--countries", type=str, default="IN,TR,RU,IR,SA,AE,EG,IQ,LB,ET,UG,SD,NG,KE,CN,PK,MM,TH,VN,SY,CU")
    ap.add_argument("--timeout", type=int, default=90)
    ap.add_argument("--retries", type=int, default=4)
    ap.add_argument("--sleep", type=float, default=1.25, help="Base delay between requests.")
    ap.add_argument("--out_json", type=str, default="output/netblocks_platform_cases.json")
    ap.add_argument("--out_jsonl", type=str, default="output/netblocks_platform_cases.jsonl")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out_json) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(args.out_jsonl) or ".", exist_ok=True)

    keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
    kw_re = compile_keyword_regex(keywords)

    iso2s = [c.strip().upper() for c in args.countries.split(",") if c.strip()]
    iso_to_name = iso2_to_name_map(iso2s)
    country_names = list(iso_to_name.values())
    countries_re = compile_country_regex(country_names + iso2s)  # match either name or code

    session = make_session(timeout=args.timeout, retries=args.retries)

    seen = set()
    results = []

    # JSONL stream writer
    jsonl_fh = open(args.out_jsonl, "w", encoding="utf-8")

    try:
        for p in range(1, args.pages + 1):
            list_url = LISTING_URL.format(page=p)
            try:
                soup = get_soup(session, list_url, args.timeout)
            except requests.RequestException as e:
                print(f"[warn] failed to fetch listing p={p}: {e}")
                time.sleep(args.sleep * (1.0 + 0.25 * random.random()))
                continue

            articles = soup.select("article")
            print(f"[info] page {p}: found {len(articles)} articles")

            for art in articles:
                h = art.select_one("h2")
                title = text_or_none(h)
                a = art.select_one("a")
                url = a["href"].strip() if a and a.has_attr("href") else ""
                if not url or url in seen:
                    continue

                # Skip if no platform keyword in title
                if not kw_re.search(title):
                    continue

                # Listing date/snippet (fallbacks)
                t = art.select_one("time")
                list_date = t.get("datetime").strip() if (t and t.has_attr("datetime")) else ""
                snippet = clean_spaces(art.get_text(" ", strip=True))[:400]

                # Visit detail page for better date + full text
                try:
                    meta = parse_article_detail(session, url, args.timeout)
                except requests.RequestException as e:
                    print(f"[warn] failed to fetch article: {url} ({e})")
                    meta = {}

                published_at = meta.get("published_at", "") or list_date
                full_text = meta.get("full_text", "")
                excerpt = meta.get("excerpt", snippet)

                # Match platforms & countries from title + full text
                hay = f"{title}\n{full_text}"
                platforms = sorted(set(m.group(0) for m in kw_re.finditer(hay)))
                countries_found = sorted(set(m.group(0) for m in countries_re.finditer(hay)))

                # Normalize countries_found to canonical (prefer ISO2 if name matched)
                normalized_countries = []
                for found in countries_found:
                    # map names back to codes if possible
                    # reverse map
                    name_to_iso = {v.lower(): k for k, v in iso_to_name.items()}
                    iso_guess = name_to_iso.get(found.lower())
                    normalized_countries.append(iso_guess or found.upper())

                rec = {
                    "title": title,
                    "url": url,
                    "published_at": published_at,
                    "snippet": snippet,
                    "excerpt": excerpt,
                    "platform_matches": platforms,
                    "country_matches": normalized_countries,
                }

                seen.add(url)
                results.append(rec)
                jsonl_fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

                # polite delay with jitter
                time.sleep(args.sleep * (1.0 + 0.25 * random.random()))

        # Save pretty JSON summary
        with open(args.out_json, "w", encoding="utf-8") as jf:
            json.dump(results, jf, ensure_ascii=False, indent=2)

        print(f"[ok] saved {len(results)} results → {args.out_json} and {args.out_jsonl}")

    finally:
        jsonl_fh.close()


if __name__ == "__main__":
    main()
