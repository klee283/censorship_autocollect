#!/usr/bin/env python3
import argparse, csv, json, sys
from datetime import date

SCHEMA = [
    "case_id","country","iso2","start_date","end_date","platform","platform_domain","platform_owner",
    "method_blocking","scope","status","official_reason_text","official_reason_category",
    "suspected_motives","legal_basis","event_context","detected_by","evidence_urls","asn_list",
    "features_anonymity","features_recommendation","features_encryption","features_real_name_policy",
    "features_availability","features_registration","features_revenue_model","features_fee_model",
    "regime_type_source","regime_type_value","notes","last_updated"
]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_jsonl", required=True)
    ap.add_argument("--out_csv", required=True)
    ap.add_argument("--touch_last_updated", action="store_true")
    args = ap.parse_args()

    # read all jsonl
    rows = []
    with open(args.in_jsonl, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            obj = json.loads(line)
            if args.touch_last_updated:
                obj["last_updated"] = date.today().isoformat()
            rows.append([obj.get(k, "") for k in SCHEMA])

    # append to CSV (create header if file empty)
    write_header = False
    try:
        with open(args.out_csv, "r", encoding="utf-8") as _:
            pass
    except FileNotFoundError:
        write_header = True

    with open(args.out_csv, "a", newline="", encoding="utf-8") as out:
        w = csv.writer(out)
        if write_header:
            w.writerow(SCHEMA)
        w.writerows(rows)

    print(f"Appended {len(rows)} cases to {args.out_csv}")

if __name__ == "__main__":
    main()
