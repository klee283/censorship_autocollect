
# Censorship Auto-Collection Toolkit

This repo helps you **automatically collect government-level platform censorship cases** and convert them into a **clean, analysis-ready dataset**.

---

## Folder Structure (Final)

```
censorship_autocollect/
│
├── data/
│   ├── case_schema.csv                # REQUIRED: master dataset columns
│   ├── features_reference.csv         # REQUIRED: platform feature taxonomy
│   └── platform_profile.csv           # OPTIONAL: user-provided metadata
│
├── output/                            # directory to save outputs
│   ├── ooni_results_*.csv
│   ├── stop_platform_cases.csv
│   ├── netblocks_platform_cases.json
│   ├── cases_llm.jsonl
│   ├── case_schema_annotated.csv
│
├── scripts/
│   ├── ooni_fetch.py                  # REQUIRED: robust OONI API fetcher
│   ├── accessnow_stop_fetch.py        # REQUIRED: AccessNow STOP filter
│   ├── netblocks_scrape.py            # REQUIRED: NetBlocks report scraper
│   ├── jsonl_to_csv.py                # Append LLM JSON → case_schema.csv
│   └── annotate_cases_with_features.py# Join platform metadata
│
├── llm_prompts/
│   └── case_label_prompt.txt          # REQUIRED: convert incident → JSON
│
└── README.md
```

---

## Quickstart

```
python -V   # Python 3.9–3.12
pip install requests pandas beautifulsoup4 python-dateutil
```

---

# Workflow Overview

1) **OONI measurements →** `output/ooni_results.csv`  
2) **STOP shutdowns →** `output/stop_platform_cases.csv`  
3) **NetBlocks reports →** `output/netblocks_platform_cases.json`  
4) **LLM conversion (prompt)**: → `output/cases_llm.jsonl`  
5) **Append to master**: JSONL → `data/case_schema.csv`  
6) **(Optional) Feature annotation**: join `platform_profile.csv` → `output/case_schema_annotated.csv`  
7) **Analyze** (notebook/script)

---

# Step-by-Step

## Step 1 — OONI Measurements

```
python scripts/ooni_fetch_windowed.py \
  --domains tiktok.com,twitter.com,facebook.com,instagram.com,youtube.com,whatsapp.com,telegram.org,reddit.com,snapchat.com,linkedin.com,wechat.com,vk.com,signal.org,pinterest.com,discord.com,tumblr.com,line.me,medium.com,viber.com,threads.net \
  --countries IN,TR,RU,IR,SA,AE,EG,IQ,LB,ET,UG,SD,NG,KE,CN,PK,MM,TH,VN,SY,CU \
  --since 2020-01-01 \
  --until 2025-10-30 \
  --limit 200 --timeout 180 --retries 5 --sleep 1.5 \
  --confirmed_only \
  --out output/ooni_results_blocked_window.csv
```

### Useful Flags
| Flag | Meaning |
|------|--------|
| `--confirmed_only` | Only confirmed blocks |
| `--limit` | Per request page size |
| `--timeout` | Request timeout |
| `--retries` | Retry attempts |
| `--sleep` | Delay per page |

💡 Start with few domains/countries → scale up

---

## Step 2 — AccessNow STOP Filter

```
python scripts/accessnow_stop_fetch.py \
  --csv /path/to/STOP.csv \
  --out output/stop_platform_cases.csv \
  --keywords "Twitter,Facebook,TikTok,YouTube,Telegram,Instagram,WhatsApp,Signal,Snapchat,Reddit"
```

---

## Step 3 — NetBlocks Reports

```
python scripts/netblocks_scrape.py \
  --pages 5 \
  --out output/netblocks_platform_cases.json
```

---

## Step 4 — LLM Case Conversion → JSONL

Convert incident text → structured JSON with:
```
llm_prompts/case_label_prompt.txt
```

Each JSON record → **one line**:
```
output/cases_llm.jsonl
```

Example:
```json
{"case_id":"IN-20200629-TIKTOK","country":"India","platform":"TikTok","start_date":"2020-06-29"}
```

---

## Step 5 — Append JSON → Master CSV

```
python scripts/jsonl_to_csv.py \
  --in_jsonl output/cases_llm.jsonl \
  --out_csv data/case_schema.csv \
  --touch_last_updated
```

---

## Step 6 — Optional: Feature Annotation

Create once:

```
data/platform_profile.csv
```

Then run:
```
python scripts/annotate_cases_with_features.py \
  --cases_csv data/case_schema.csv \
  --profile_csv data/platform_profile.csv \
  --out_csv output/case_schema_annotated.csv
```

---

# Tips & Troubleshooting

- If OONI runs fast → likely low matches  
- Avoid timeouts → reduce date window, limit, split countries  
- Save raw pull results under `output/`  
- Ensure JSON keys match `case_schema.csv`

---

# Ethics & Notes
- Respect robots.txt and API rate limits
- Keep original source URLs
- Document LLM + settings

---

Happy researching!
