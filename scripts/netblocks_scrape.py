#!/usr/bin/env python3
"""
Scrape NetBlocks report pages for platform blocking incidents.

Example:
python netblocks_scrape.py --pages 5 --out netblocks_cases.json
"""

import argparse
import re
import requests
import json
from bs4 import BeautifulSoup

BASE = "https://netblocks.org/reports/page/{page}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=3)
    ap.add_argument(
        "--keywords",
        type=str,
        default="Twitter,Facebook,TikTok,YouTube,Telegram,Instagram,WhatsApp,Signal,Snapchat,Reddit"
    )
    ap.add_argument("--out", default="netblocks_platform_cases.json")
    args = ap.parse_args()

    keywords = [k.strip() for k in args.keywords.split(",")]
    pat = re.compile("|".join([re.escape(k) for k in keywords]), re.I)

    results = []

    for p in range(1, args.pages + 1):
        url = BASE.format(page=p)
        r = requests.get(url, timeout=60)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")
        for article in soup.select("article"):
            h = article.select_one("h2")
            title = h.get_text(strip=True) if h else ""

            if not pat.search(title):
                continue

            a = article.select_one("a")
            link = a["href"] if a else ""
            t = article.select_one("time")
            date = t.get("datetime") if t else ""

            snippet = article.get_text(" ", strip=True)[:400]

            results.append({
                "title": title,
                "url": link,
                "date": date,
                "snippet": snippet
            })

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(results)} results → {args.out}")


if __name__ == "__main__":
    main()
