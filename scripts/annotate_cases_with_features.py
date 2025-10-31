#!/usr/bin/env python3
import argparse
import pandas as pd

FEATURE_COLS = [
    "features_anonymity","features_recommendation","features_encryption","features_real_name_policy",
    "features_availability","features_registration","features_revenue_model","features_fee_model"
]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cases_csv", required=True, help="data/case_schema.csv (growing master)")
    ap.add_argument("--profile_csv", required=True, help="data/platform_profile.csv")
    ap.add_argument("--out_csv", required=True, help="data/case_schema_annotated.csv")
    args = ap.parse_args()

    cases = pd.read_csv(args.cases_csv, dtype=str).fillna("")
    prof  = pd.read_csv(args.profile_csv, dtype=str).fillna("")

    merged = cases.drop(columns=[c for c in FEATURE_COLS if c in cases.columns], errors="ignore")
    merged = merged.merge(
        prof[["platform","platform_domain","platform_owner"] + FEATURE_COLS],
        how="left", on=["platform","platform_domain","platform_owner"]
    )

    merged.to_csv(args.out_csv, index=False)
    print(f"Wrote annotated: {args.out_csv}  (rows={len(merged)})")

if __name__ == "__main__":
    main()
