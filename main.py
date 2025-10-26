#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Exports raw OData v3 data via secure proxy to CSVs:
- clients.csv
- sales.csv
- returns.csv
- payments.csv
- last_scrape.csv (dd.mm.yyyy hh:mm, UTC+5)

Poka-yoke:
- Fails fast if PROXY_URL/PROXY_TOKEN missing
- Retries transient HTTP errors
- Pagination with $top/$skip
- Per-entity isolation (others still export if one fails)
"""

import os
import sys
import csv
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode
import requests
from lxml import etree

PROXY_URL = os.getenv("PROXY_URL", "").rstrip("/")   # e.g. https://your-proxy.onrender.com
PROXY_TOKEN = os.getenv("PROXY_TOKEN", "")

OUT_DIR = "output"
PAGE_SIZE = 1000
HTTP_TIMEOUT = 60
RETRIES = 3
SLEEP = 2

ATOM_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "m": "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
    "d": "http://schemas.microsoft.com/ado/2007/08/dataservices",
}

ENTITIES = {
    "clients": "Catalog_Контрагенты",
    "sales": "Document_РеализацияТоваровУслуг",
    "returns": "Document_ВозвратТоваровОтПокупателя",
    "payments": "Document_ПлатежноеПоручениеВходящее",
}

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
        "СуммаДокумента","ВалютаДокумента_Key","Организация_Key","ВидОперации","СуммаПлатежа_Итого"
    ],
}

def fail_if_misconfigured():
    missing = []
    if not PROXY_URL: missing.append("PROXY_URL")
    if not PROXY_TOKEN: missing.append("PROXY_TOKEN")
    if missing:
        raise SystemExit("Missing env vars: " + ", ".join(missing))

def http_get(entity, params):
    """GET via proxy with retries."""
    headers = {"X-Proxy-Token": PROXY_TOKEN, "Accept": "application/atom+xml"}
    url = f"{PROXY_URL}/odata"
    last_exc = None
    for _ in range(RETRIES):
        try:
            resp = requests.get(
                url,
                params={"path": entity, **params},
                headers=headers,
                timeout=HTTP_TIMEOUT,
            )
            # Retry only on 5xx
            if 500 <= resp.status_code < 600:
                time.sleep(SLEEP)
                continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_exc = e
            time.sleep(SLEEP)
    raise last_exc

def parse_atom_entries(xml_bytes):
    root = etree.fromstring(xml_bytes)
    return root.findall("atom:entry", ATOM_NS)

def parse_properties(entry):
    props = entry.find("atom:content/m:properties", ATOM_NS)
    flat, colls = {}, {}
    if props is None:
        return flat, colls
    for child in props:
        tag = etree.QName(child).localname
        mtype = child.attrib.get("{%s}type" % ATOM_NS["m"], "")
        if mtype.startswith("Collection("):
            colls[tag] = child
        else:
            flat[tag] = (child.text or "").strip()
    return flat, colls

def fetch_all(entity):
    skip = 0
    rows = []
    while True:
        params = {"$top": str(PAGE_SIZE), "$skip": str(skip), "$format": "atom"}
        resp = http_get(entity, params)
        entries = parse_atom_entries(resp.content)
        if not entries:
            break
        for e in entries:
            flat, colls = parse_properties(e)
            rows.append((flat, colls))
        if len(entries) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
    return rows

def sum_payment_lines(collection_elem):
    total = 0.0
    if not collection_elem:
        return total
    for el in collection_elem.findall("d:element", ATOM_NS):
        t = el.find("d:СуммаПлатежа", ATOM_NS)
        if t is not None and t.text:
            try:
                total += float(str(t.text).replace(",", "."))
            except:
                pass
    return total

def ensure_outdir():
    if not os.path.isdir(OUT_DIR):
        os.makedirs(OUT_DIR, exist_ok=True)

def write_csv(path, rows, columns):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in columns})

def almaty_now():
    return datetime.now(timezone(timedelta(hours=5))).strftime("%d.%m.%Y %H:%M")

def extract_clients():
    data = []
    for flat, _ in fetch_all(ENTITIES["clients"]):
        data.append({k: flat.get(k, "") for k in COLUMNS["clients"]})
    return data

def extract_sales():
    data = []
    for flat, _ in fetch_all(ENTITIES["sales"]):
        data.append({k: flat.get(k, "") for k in COLUMNS["sales"]})
    return data

def extract_returns():
    data = []
    for flat, _ in fetch_all(ENTITIES["returns"]):
        data.append({k: flat.get(k, "") for k in COLUMNS["returns"]})
    return data

def extract_payments():
    data = []
    for flat, colls in fetch_all(ENTITIES["payments"]):
        row = {k: flat.get(k, "") for k in COLUMNS["payments"] if k != "СуммаПлатежа_Итого"}
        lines = colls.get("РасшифровкаПлатежа")
        total = sum_payment_lines(lines)
        if total and not row.get("СуммаДокумента"):
            row["СуммаДокумента"] = f"{total:.2f}"
        row["СуммаПлатежа_Итого"] = f"{total:.2f}"
        data.append(row)
    return data

def main():
    fail_if_misconfigured()
    ensure_outdir()

    export_plan = [
        ("clients.csv", extract_clients, "clients"),
        ("sales.csv", extract_sales, "sales"),
        ("returns.csv", extract_returns, "returns"),
        ("payments.csv", extract_payments, "payments"),
    ]

    any_ok = False
    for fname, func, key in export_plan:
        try:
            rows = func()
            write_csv(os.path.join(OUT_DIR, fname), rows, COLUMNS[key])
            print(f"✅ {fname}: {len(rows)} rows")
            any_ok = True
        except Exception as e:
            print(f"❌ {fname} failed: {e}", file=sys.stderr)

    if any_ok:
        write_csv(
            os.path.join(OUT_DIR, "last_scrape.csv"),
            [{"last_scrape": almaty_now()}],
            ["last_scrape"],
        )
        print("🕒 last_scrape.csv written")
    else:
        raise SystemExit("All exports failed; check PROXY_URL/token or proxy health.")

if __name__ == "__main__":
    main()
