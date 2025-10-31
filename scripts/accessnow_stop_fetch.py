#!/usr/bin/env python3
"""
Filter the AccessNow STOP shutdown dataset for social media platforms.

Example:
python accessnow_stop_fetch.py \
  --csv STOP_latest.csv \
  --out stop_platform_cases.csv
"""

import argparse
import pandas as pd
import re


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--out", default="stop_platform_cases.csv")
    ap.add_argument(
        "--keywords",
        type=str,
        default="Twitter,Facebook,TikTok,YouTube,Telegram,Instagram,WhatsApp,Signal,Snapchat,Reddit"
    )
    args = ap.parse_args()

    keywords = [k.strip() for k in args.keywords.split(",")]
    pattern = re.compile("|".join([re.escape(k) for k in keywords]), re.I)

    df = pd.read_csv(args.csv)

    # Possible text columns that mention platform
    text_cols = [
        c for c in df.columns
        if any(k in c.lower() for k in ["title", "descr", "notes", "summary", "narrative"])
    ]
    if not text_cols:
        text_cols = df.columns.tolist()

    mask = df[text_cols].astype(str).apply(
        lambda s: s.str.contains(pattern, regex=True, na=False)
    ).any(axis=1)

    out = df[mask].copy()
    out.to_csv(args.out, index=False)

    print(f"Found {mask.sum()} rows → {args.out}")


if __name__ == "__main__":
    main()
