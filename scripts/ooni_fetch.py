#!/usr/bin/env python3
"""
Robust OONI fetcher with retries/backoff and tunable chunk size/timeouts.

Example (comma-separated domains & countries):

python scripts/ooni_fetch.py \
  --domains tiktok.com,twitter.com,facebook.com,instagram.com,youtube.com,whatsapp.com,telegram.org,reddit.com,snapchat.com,linkedin.com,wechat.com,vk.com,signal.org,pinterest.com,discord.com,tumblr.com,line.me,medium.com,viber.com,threads.net \
  --countries IN,TR,RU,IR,SA,AE,EG,IQ,LB,ET,UG,SD,NG,KE,CN,PK,MM,TH,VN,SY,CU \
  --since 2020-01-01 \
  --until 2025-10-30 \
  --limit 400 --timeout 120 --retries 5 --sleep 1.5 \
  --confirmed_only \
  --out output/ooni_results_blocked.csv
"""

import argparse
import csv
import time
import requests
import datetime as dt
from requests.adapters import HTTPAdapter, Retry

API = "https://api.ooni.io/api/v1/measurements"


def make_session(timeout: int, retries: int) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=1.5,  # exponential backoff
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.timeout = timeout
    return s


def request_page(session: requests.Session, params: dict) -> dict:
    r = session.get(API, params=params, timeout=session.timeout)
    r.raise_for_status()
    return r.json()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--domains",
        type=str,
        required=True,
        help="Comma-separated list, e.g. 'tiktok.com,twitter.com,...' (spaces after commas are fine)",
    )
    ap.add_argument(
        "--countries",
        type=str,
        required=True,
        help="Comma-separated ISO2 codes, e.g. 'IN,TR,RU,...' (spaces after commas are fine)",
    )
    ap.add_argument("--since", type=str, default="2018-01-01")
    ap.add_argument("--until", type=str, default=dt.date.today().isoformat())
    ap.add_argument("--test_name", type=str, default="web_connectivity")
    ap.add_argument("--limit", type=int, default=300)
    ap.add_argument("--sleep", type=float, default=1.0)
    ap.add_argument("--timeout", type=int, default=90)
    ap.add_argument("--retries", type=int, default=3)
    # Optional filter
    ap.add_argument(
        "--confirmed_only",
        action="store_true",
        help="Only include measurements marked confirmed by OONI",
    )
    ap.add_argument("--out", type=str, default="ooni_results.csv")
    args = ap.parse_args()

    # Parse comma-separated inputs (allow spaces after commas)
    domains = [d.strip() for d in args.domains.split(",") if d.strip()]
    countries = [c.strip().upper() for c in args.countries.split(",") if c.strip()]

    header = [
        "domain",
        "country",
        "measurement_start_time",
        "anomaly",
        "confirmed",
        "blocking_country",
        "probe_asn",
        "probe_cc",
        "failure",
        "measurement_url",
    ]

    session = make_session(timeout=args.timeout, retries=args.retries)

    with open(args.out, "w", newline="", encoding="utf-8") as out:
        w = csv.writer(out)
        w.writerow(header)

        for domain in domains:
            for country in countries:
                offset = 0
                while True:
                    params = {
                        "domain": domain,
                        "probe_cc": country,
                        "since": args.since,
                        "until": args.until,
                        "test_name": args.test_name,
                        "limit": args.limit,
                        "offset": offset,
                    }
                    if args.confirmed_only:
                        params["confirmed"] = "true"

                    try:
                        data = request_page(session, params)
                    except requests.exceptions.ReadTimeout:
                        print(f"timeout at offset {offset}; backing off...")
                        time.sleep(max(args.sleep * 2, 3.0))
                        continue
                    except requests.exceptions.HTTPError as e:
                        code = getattr(e.response, "status_code", "unknown")
                        print(f"HTTP {code} at offset {offset}; sleeping then continuing...")
                        time.sleep(max(args.sleep * 2, 3.0))
                        continue
                    except requests.exceptions.RequestException as e:
                        print(f"Request error at offset {offset}: {e}; backing off...")
                        time.sleep(max(args.sleep * 2, 3.0))
                        continue

                    results = data.get("results", [])
                    for m in results:
                        w.writerow(
                            [
                                domain,
                                country,
                                m.get("measurement_start_time"),
                                m.get("anomaly"),
                                m.get("confirmed"),
                                m.get("blocking_country"),
                                m.get("probe_asn"),
                                m.get("probe_cc"),
                                m.get("failure"),
                                m.get("measurement_url"),
                            ]
                        )

                    if len(results) < args.limit:
                        break
                    offset += args.limit
                    time.sleep(args.sleep)

    print(f"Done → wrote: {args.out}")


if __name__ == "__main__":
    main()
