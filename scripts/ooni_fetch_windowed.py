#!/usr/bin/env python3
"""
Robust OONI fetcher: time-windowed paging (default: monthly) to avoid deep-offset stalls.
- Resilient retries/backoff with jitter
- Checkpoint/resume by (domain, country, window)
- Optional gzip output
- Progress with counts & rough ETA
- Adaptive window splitting on repeated 5xx (only if stuck at offset 0)

Usage example:

python scripts/ooni_fetch_windowed.py \
  --domains tiktok.com,twitter.com,facebook.com,instagram.com,youtube.com,whatsapp.com,telegram.org,reddit.com,snapchat.com,linkedin.com,wechat.com,vk.com,signal.org,pinterest.com,discord.com,tumblr.com,line.me,medium.com,viber.com,threads.net \
  --countries IN,TR,RU,IR,SA,AE,EG,IQ,LB,ET,UG,SD,NG,KE,CN,PK,MM,TH,VN,SY,CU \
  --since 2020-01-01 --until 2025-10-30 \
  --limit 200 --timeout 180 --retries 5 --sleep 1.5 \
  --confirmed_only \
  --out output/ooni_results_blocked_window.csv
"""

import argparse
import csv
import datetime as dt
import gzip
import json
import os
import random
import sys
import time
from typing import Dict, Tuple, List

import requests
from requests.adapters import HTTPAdapter, Retry

API = "https://api.ooni.io/api/v1/measurements"

# ------------------------ HTTP session & requests ------------------------

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
    s.timeout = timeout
    return s

def request_page(session: requests.Session, params: dict) -> dict:
    r = session.get(API, params=params, timeout=session.timeout)
    r.raise_for_status()
    return r.json()

def get_count(session: requests.Session, base_params: dict) -> int:
    """Probe metadata.count for this query window (cheap: limit=1)."""
    probe = dict(base_params)
    probe["limit"] = 1
    probe["offset"] = 0
    data = request_page(session, probe)
    meta = data.get("metadata", {})
    try:
        return int(meta.get("count", 0))
    except Exception:
        return 0

# ------------------------ Time window utilities ------------------------

def month_windows(since_str: str, until_str: str) -> List[Tuple[str, str]]:
    since = dt.date.fromisoformat(since_str)
    until = dt.date.fromisoformat(until_str)
    # normalize to first day of month for start
    cur = since.replace(day=1)
    out = []
    while cur <= until:
        # end of month
        if cur.month == 12:
            month_end = dt.date(cur.year, 12, 31)
        else:
            first_next = dt.date(cur.year, cur.month + 1, 1)
            month_end = first_next - dt.timedelta(days=1)
        end = min(month_end, until)
        # ensure we don't start before user's since if since isn't first of month
        start = max(cur, since)
        out.append((start.isoformat(), end.isoformat()))
        cur = end + dt.timedelta(days=1)
    return out

def split_window(win_since: str, win_until: str) -> Tuple[Tuple[str, str], Tuple[str, str]]:
    s = dt.date.fromisoformat(win_since)
    e = dt.date.fromisoformat(win_until)
    if s >= e:
        return (s.isoformat(), e.isoformat()), None
    mid = s + (e - s) // 2
    left = (s.isoformat(), mid.isoformat())
    right_start = (mid + dt.timedelta(days=1)).isoformat()
    right = (right_start, e.isoformat())
    return left, right

# ------------------------ Checkpointing ------------------------

def load_ckpt(path: str) -> Dict:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"version": 1, "pairs": {}}

def save_ckpt(path: str, ckpt: Dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(ckpt, f)
    os.replace(tmp, path)

def get_pair_node(ckpt: Dict, domain: str, country: str) -> Dict:
    key = f"{domain}|{country}"
    pairs = ckpt.setdefault("pairs", {})
    return pairs.setdefault(key, {"windows": {}})

def get_win_key(since: str, until: str) -> str:
    return f"{since}|{until}"

# ------------------------ CSV writer ------------------------

def open_csv_writer(path: str, header: List[str], gzip_out: bool):
    # Create parent dirs
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    exists = os.path.exists(path)
    is_nonempty = exists and os.path.getsize(path) > 0

    if gzip_out:
        fh = gzip.open(path, "at", newline="", encoding="utf-8")
    else:
        fh = open(path, "a", newline="", encoding="utf-8")
    w = csv.writer(fh)
    if not is_nonempty:
        w.writerow(header)
    return fh, w

# ------------------------ Main logic ------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--domains", type=str, required=True,
                    help="Comma-separated list, spaces after commas are fine.")
    ap.add_argument("--countries", type=str, required=True,
                    help="Comma-separated ISO2 codes, spaces after commas are fine.")
    ap.add_argument("--since", type=str, default="2018-01-01")
    ap.add_argument("--until", type=str, default=dt.date.today().isoformat())
    ap.add_argument("--test_name", type=str, default="web_connectivity")
    ap.add_argument("--limit", type=int, default=200, help="Page size per request.")
    ap.add_argument("--sleep", type=float, default=1.0, help="Base sleep between pages (seconds).")
    ap.add_argument("--timeout", type=int, default=120)
    ap.add_argument("--retries", type=int, default=5)
    ap.add_argument("--confirmed_only", action="store_true",
                    help="Only include measurements marked confirmed by OONI.")
    ap.add_argument("--out", type=str, default="ooni_results.csv",
                    help="Output CSV path (single file).")
    ap.add_argument("--gzip", action="store_true", help="Write gzipped CSV (.csv.gz).")
    ap.add_argument("--ckpt", type=str, default="ooni_ckpt.json",
                    help="Checkpoint file path.")
    ap.add_argument("--max_5xx_streak", type=int, default=5,
                    help="Consecutive 5xx before splitting a window (only if stuck at offset 0).")
    args = ap.parse_args()

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

    gzip_out = args.gzip or args.out.endswith(".gz")
    out_path = args.out
    if gzip_out and not out_path.endswith(".gz"):
        out_path = out_path + ".gz"

    session = make_session(timeout=args.timeout, retries=args.retries)
    ckpt = load_ckpt(args.ckpt)

    fh, writer = open_csv_writer(out_path, header, gzip_out)
    try:
        for domain in domains:
            for country in countries:
                pair_node = get_pair_node(ckpt, domain, country)
                # Build initial monthly windows
                base_windows = month_windows(args.since, args.until)

                # Prepare a stack of windows to process (oldest first)
                pending = []
                for ws, we in base_windows:
                    wkey = get_win_key(ws, we)
                    wstate = pair_node["windows"].get(wkey, {"offset": 0, "done": False})
                    if not wstate.get("done", False):
                        pending.append((ws, we))

                while pending:
                    win_since, win_until = pending.pop(0)
                    wkey = get_win_key(win_since, win_until)
                    wstate = pair_node["windows"].get(wkey, {"offset": 0, "done": False})
                    offset = int(wstate.get("offset", 0))
                    done = bool(wstate.get("done", False))
                    if done:
                        continue

                    base_params = {
                        "domain": domain,
                        "probe_cc": country,
                        "since": win_since,
                        "until": win_until,
                        "test_name": args.test_name,
                    }
                    if args.confirmed_only:
                        base_params["confirmed"] = "true"

                    # Probe count for progress display
                    try:
                        total_count = get_count(session, base_params)
                    except requests.exceptions.RequestException:
                        total_count = 0

                    if total_count:
                        pages = (total_count + args.limit - 1) // args.limit
                        print(f"[{domain} {country}] {win_since}..{win_until} → {total_count} rows ≈ {pages} pages")
                    else:
                        print(f"[{domain} {country}] {win_since}..{win_until} → (count unknown or zero)")

                    five_xx_streak = 0
                    pages_done = 0 if offset == 0 else offset // max(args.limit, 1)
                    wrote_any_in_this_window = offset > 0

                    while True:
                        params = dict(base_params)
                        params["limit"] = args.limit
                        params["offset"] = offset

                        try:
                            t0 = time.time()
                            data = request_page(session, params)
                            dur = time.time() - t0
                            results = data.get("results", [])
                            # Write rows
                            for m in results:
                                writer.writerow([
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
                                ])
                            fh.flush()

                            # progress bookkeeping
                            got = len(results)
                            if total_count:
                                done_rows = min(offset + got, total_count)
                                pages_done = (offset // args.limit) + (1 if got > 0 else 0)
                                print(f"[{domain} {country}] {win_since}..{win_until} "
                                      f"{done_rows}/{total_count} rows "
                                      f"(page {pages_done}) in {dur:.1f}s")

                            # update checkpoint
                            offset += got
                            pair_node["windows"][wkey] = {"offset": offset, "done": False}
                            save_ckpt(args.ckpt, ckpt)

                            five_xx_streak = 0
                            wrote_any_in_this_window = wrote_any_in_this_window or (got > 0)

                            # page boundary or window complete
                            if got < args.limit:
                                # finished this window
                                pair_node["windows"][wkey] = {"offset": offset, "done": True}
                                save_ckpt(args.ckpt, ckpt)
                                break

                            # polite randomized sleep
                            time.sleep(args.sleep * (1.0 + random.random()))

                        except requests.exceptions.HTTPError as e:
                            code = getattr(e.response, "status_code", None)
                            if code and 500 <= code < 600:
                                five_xx_streak += 1
                            else:
                                five_xx_streak = 0
                            print(f"HTTP {code or '??'} at offset {offset}; sleeping then continuing...")
                            time.sleep(max(args.sleep * 2, 3.0) * (1.0 + 0.25 * random.random()))

                            # If we are stuck at the *very first page* of this window (offset==0) and keep 5xx-ing,
                            # split the window into halves and try each half from offset 0.
                            if offset == 0 and five_xx_streak >= args.max_5xx_streak:
                                left, right = split_window(win_since, win_until)
                                # mark this window as "skipped" so we don't loop forever
                                pair_node["windows"][wkey] = {"offset": 0, "done": True}
                                save_ckpt(args.ckpt, ckpt)
                                if left and left != (win_since, win_until):
                                    pending.insert(0, left)
                                if right and right != (win_since, win_until):
                                    pending.insert(1 if left else 0, right)
                                print(f"Split window {win_since}..{win_until} into {left} and {right} due to repeated 5xx.")
                                break
                            continue

                        except requests.exceptions.ReadTimeout:
                            print(f"timeout at offset {offset}; backing off...")
                            time.sleep(max(args.sleep * 2, 3.0) * (1.0 + 0.25 * random.random()))
                            continue

                        except requests.exceptions.RequestException as e:
                            print(f"Request error at offset {offset}: {e}; backing off...")
                            time.sleep(max(args.sleep * 2, 3.0) * (1.0 + 0.25 * random.random()))
                            continue

        print(f"Done → wrote: {out_path}")
    finally:
        fh.close()

if __name__ == "__main__":
    # Tip: on macOS, use `caffeinate -i` to prevent idle sleep while this runs.
    # Example: caffeinate -i python3 scripts/ooni_fetch_windowed.py ...
    main()
