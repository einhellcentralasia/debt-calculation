#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
1C OData v3 → CSV exporter (raw datasets)
- Clients, Sales, Returns, Payments
- Overwrites /data/*.csv each run
- Writes last_scrape.csv with UTC+5 timestamp (dd.mm.yyyy hh:mm)

Safety (poka-yoke):
- Env-based secrets only (no hardcoded creds)
- Timeouts & retries
- Pagination via $top/$skip
- Graceful partial failures (per-entity try/except)
"""

import os
import sys
import csv
import time
import math
from datetime import datetime, timezone, timedelta
from urllib.parse import quote
import requests
from lxml import etree

# ---------- Config ----------
ODATA_BASE = os.getenv("ODATA_URL", "").rstrip("/")
ODATA_USER = os.getenv("ODATA_USER", "")
ODATA_PASS = os.getenv("ODATA_PASS", "")

# entity paths with Cyrillic segments must be URL-encoded
ENTITIES = {
    "clients": "Catalog_" + quote("Контрагенты"),
    "sales": "Document_" + quote("РеализацияТоваровУслуг"),
    "returns": "Document_" + quote("ВозвратТоваровОтПокупателя"),
    "payments": "Document_" + quote("ПлатежноеПоручениеВходящее"),
}

OUT_DIR = "data"
PAGE_SIZE = 1000
HTTP_TIMEOUT = 60
RETRIES = 3
SLEEP_BETWEEN_RETRIES = 2

# Desired columns (raw but focused)
COLUMNS = {
    "clients": [
        "Ref_Key","Code","Description","НаименованиеПолное","ИдентификационныйКодЛичности",
        "КБЕ","ГоловнойКонтрагент_Key","ОсновнойДоговорКонтрагента_Key","СтранаРезидентства_Key",
        "DeletionMark","IsFolder","Parent_Key"
    ],
    "sales": [
        "Ref_Key","Number","Date","Posted","Контрагент_Key","ДоговорКонтрагента_Key",
        "СуммаДокумента","ВалютаДокумента_Key","Организация_Key"
    ],
    "returns": [
        "Ref_Key","Number","Date","Posted","Контрагент_Key","ДоговорКонтрагента_Key",
        "СуммаДокумента","ВалютаДокумента_Key","Организация_Key","ДокументОснование","ДокументОснование_Type"
    ],
    "payments": [
        "Ref_Key","Number","Date","Posted","Контрагент_Key","ДоговорКонтрагента_Key",
        "СуммаДокумента","ВалютаДокумента_Key","Организация_Key","ВидОперации",
        # derived:
        "СуммаПлатежа_Итого"
    ],
}

ATOM_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "m": "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
    "d": "http://schemas.microsoft.com/ado/2007/08/dataservices",
}

# ---------- Helpers ----------

def fail_if_misconfigured():
    miss = []
    if not ODATA_BASE: miss.append("ODATA_URL")
    if not ODATA_USER: miss.append("ODATA_USER")
    if not ODATA_PASS: miss.append("ODATA_PASS")
    if miss:
        raise SystemExit(f"Missing required environment variables (GitHub Secrets): {', '.join(miss)}")

def url_join(*parts):
    return "/".join(p.strip("/") for p in parts)

def http_get(url, params=None):
    last_exc = None
    for attempt in range(1, RETRIES + 1):
        try:
            resp = requests.get(
                url,
                params=params or {},
                auth=(ODATA_USER, ODATA_PASS),
                timeout=HTTP_TIMEOUT,
                headers={"Accept": "application/atom+xml"}
            )
            if resp.status_code >= 500:
                # Transient server issues → retry
                time.sleep(SLEEP_BETWEEN_RETRIES)
                continue
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            time.sleep(SLEEP_BETWEEN_RETRIES)
    # bubble last error
    if last_exc:
        raise last_exc
    raise RuntimeError("Unknown HTTP error")

def parse_atom_entries(xml_bytes):
    root = etree.fromstring(xml_bytes)
    return root.findall("atom:entry", ATOM_NS)

def parse_properties(entry):
    props = entry.find("atom:content/m:properties", ATOM_NS)
    if props is None:
        return {}, {}
    flat = {}
    collections = {}
    for child in props:
        tag = etree.QName(child).localname  # e.g., 'Ref_Key'
        # If it's a collection, capture elements separately
        type_attr = child.attrib.get("{%s}type" % ATOM_NS["m"], "")
        if type_attr.startswith("Collection("):
            # store raw XML element for later specific parsing (payments lines, etc.)
            collections[tag] = child
            continue
        # otherwise scalar → text
        flat[tag] = (child.text or "").strip()
    return flat, collections

def fetch_all(entity_path):
    skip = 0
    results = []
    while True:
        url = url_join(ODATA_BASE, entity_path)
        params = {"$format": "atom", "$top": str(PAGE_SIZE), "$skip": str(skip)}
        resp = http_get(url, params=params)
        entries = parse_atom_entries(resp.content)
        if not entries:
            break
        for e in entries:
            flat, colls = parse_properties(e)
            results.append((flat, colls))
        if len(entries) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
    return results

def sum_payments_from_lines(collection_elem):
    # For payments, we sum <d:СуммаПлатежа> across lines
    total = 0.0
    if collection_elem is None:
        return total
    for el in collection_elem.findall("d:element", ATOM_NS):
        amt_node = el.find("d:СуммаПлатежа", ATOM_NS)
        if amt_node is not None and amt_node.text:
            try:
                total += float(str(amt_node.text).replace(",", "."))
            except:
                pass
    return total

def ensure_outdir():
    if not os.path.isdir(OUT_DIR):
        os.makedirs(OUT_DIR, exist_ok=True)

def write_csv(path, rows, columns):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in columns})

def almaty_now_str():
    # UTC+5 fixed
    tz = timezone(timedelta(hours=5))
    return datetime.now(tz).strftime("%d.%m.%Y %H:%M")

# ---------- Entity extractors ----------

def extract_clients():
    data = []
    for flat, _ in fetch_all(ENTITIES["clients"]):
        row = {k: flat.get(k, "") for k in COLUMNS["clients"]}
        data.append(row)
    return data

def extract_sales():
    data = []
    for flat, _ in fetch_all(ENTITIES["sales"]):
        row = {k: flat.get(k, "") for k in COLUMNS["sales"]}
        data.append(row)
    return data

def extract_returns():
    data = []
    for flat, _ in fetch_all(ENTITIES["returns"]):
        row = {k: flat.get(k, "") for k in COLUMNS["returns"]}
        data.append(row)
    return data

def extract_payments():
    data = []
    for flat, colls in fetch_all(ENTITIES["payments"]):
        row = {k: flat.get(k, "") for k in COLUMNS["payments"] if k != "СуммаПлатежа_Итого"}
        # sum lines from РасшифровкаПлатежа (if present)
        lines = colls.get("РасшифровкаПлатежа")
        total_lines = sum_payments_from_lines(lines)
        if total_lines and not flat.get("СуммаДокумента"):
            # fallback if header amount is empty
            row["СуммаДокумента"] = f"{total_lines:.2f}"
        row["СуммаПлатежа_Итого"] = f"{total_lines:.2f}"
        data.append(row)
    return data

# ---------- Main ----------

def main():
    fail_if_misconfigured()
    ensure_outdir()

    exports = [
        ("clients.csv", extract_clients, "clients"),
        ("sales.csv", extract_sales, "sales"),
        ("returns.csv", extract_returns, "returns"),
        ("payments.csv", extract_payments, "payments"),
    ]

    any_ok = False
    for fname, func, key in exports:
        try:
            rows = func()
            write_csv(os.path.join(OUT_DIR, fname), rows, COLUMNS[key])
            print(f"✅ Wrote {fname}: {len(rows)} rows")
            any_ok = True
        except Exception as e:
            # don't crash whole run — report and continue
            print(f"❌ Failed {fname}: {e}", file=sys.stderr)

    # Always write last_scrape.csv if at least one dataset succeeded
    if any_ok:
        ts = almaty_now_str()
        write_csv(os.path.join(OUT_DIR, "last_scrape.csv"),
                  [{"last_scrape": ts}], ["last_scrape"])
        print(f"🕒 last_scrape.csv -> {ts}")
    else:
        # If everything failed, make the failure explicit
        raise SystemExit("All entity exports failed. Check credentials / URL / network.")

if __name__ == "__main__":
    main()
