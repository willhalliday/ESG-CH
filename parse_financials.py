#!/usr/bin/env python3
import warnings
import io
import re
import csv
from zipfile import ZipFile
from pathlib import Path
from lxml import etree
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import pandas as pd
from tqdm import tqdm

# ─ suppress XML-as-HTML warning ───────────────────────────────────────────────
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# ── Configuration ────────────────────────────────────────────────────────────
ZIP_PATH = Path(r"C:\Users\WillH\ch\downloads\monthly\Accounts_Monthly_Data-April2024.zip")
OUT_CSV  = Path(r"C:\Users\WillH\ch\downloads\monthly\all_metrics.csv")
# ──────────────────────────────────────────────────────────────────────────────

# Namespaces
NS = {
    'ix':   'http://www.xbrl.org/2013/inlineXBRL',
    'xbrli':'http://www.xbrl.org/2003/instance'
}

def extract_company_and_dates(f):
    """
    Extract the real company identifier and all context endDates (full YYYY-MM-DD),
    sorted descending, from this file-like object. Leaves f rewound.
    """
    contexts = {}
    company = ''
    # 1) company id
    for _, elem in etree.iterparse(f, events=('end',), tag=f"{{{NS['xbrli']}}}identifier"):
        if elem.getparent().tag == f"{{{NS['xbrli']}}}entity":
            company = (elem.text or '').strip()
    f.seek(0)

    # 2) contexts
    for _, ctx in etree.iterparse(f, events=('end',), tag=f"{{{NS['xbrli']}}}context"):
        cid = ctx.get('id')
        end_el = ctx.find('.//xbrli:endDate', namespaces=NS)
        if cid and end_el is not None and end_el.text:
            contexts[cid] = end_el.text.strip()
        ctx.clear()
    f.seek(0)

    dates = sorted(set(contexts.values()), reverse=True)
    return company, dates


def find_balance_sheet_df(html_str):
    """
    Try pandas.read_html on the literal HTML. Return the first DF with >=3 columns
    and at least 2 numeric columns, else None.
    """
    try:
        dfs = pd.read_html(io.StringIO(html_str), header=0, flavor='bs4')
    except Exception:
        return None
    for df in dfs:
        if df.shape[1] < 3:
            continue
        numcnt = 0
        for col in df.columns[1:]:
            ser = df[col].astype(str).str.replace(r'[(),]', '', regex=True)
            if pd.to_numeric(ser, errors='coerce').notna().sum() > 0:
                numcnt += 1
        if numcnt >= 2:
            return df
    return None


def process_table(df, company, dates):
    """
    Melt a pandas DF from wide to long, match year headers to full context dates,
    and build dicts for the CSV.
    """
    year_map = {}
    for col in df.columns[1:]:
        m = re.search(r'(20\d{2})', str(col))
        if m:
            year_map[col] = m.group(1)
    if not year_map:
        return []

    long = df.melt(
        id_vars=[df.columns[0]],
        value_vars=list(year_map.keys()),
        var_name='raw_period',
        value_name='raw_value'
    ).dropna(subset=['raw_value'])

    long['raw_value'] = long['raw_value'].astype(str).str.strip()
    mask = long['raw_value']\
        .str.replace(r'[(),]','',regex=True)\
        .str.match(r'-?\d+(\.\d+)?')
    long = long[mask]

    out = []
    for _, row in long.iterrows():
        label = str(row[df.columns[0]]).strip()
        tag   = re.sub(r'\W+', '_', label)
        yr    = year_map[row['raw_period']]
        period_end = next((d for d in dates if d.startswith(yr)), None)
        if period_end is None:
            continue
        val = row['raw_value'].replace(',', '').replace('(', '-').replace(')', '')
        out.append({
            'company_number': company,
            'period_end':     period_end,
            'metric_tag':     tag,
            'metric_label':   label,
            'value':          val,
            'unit':           ''
        })
    return out


def extract_table_facts(f):
    """
    BeautifulSoup fallback: scan every <table>, locate the true header row
    (>=2 valid years), skip currency row, then extract numeric cells.
    """
    f.seek(0)
    company, dates = extract_company_and_dates(f)
    f.seek(0)

    html = f.read().decode('utf-8', errors='ignore')
    soup = BeautifulSoup(html, 'html.parser')
    f.seek(0)

    year_pat = re.compile(r'\b(20\d{2})\b')
    def text(cell):
        return cell.get_text(' ', strip=True).replace('\xa0',' ').strip()

    for tbl in soup.find_all('table'):
        rows = tbl.find_all('tr')
        if not rows:
            continue

        header_idx = None
        header_years = []
        for idx, tr in enumerate(rows):
            cells = tr.find_all(['th','td'])
            if len(cells) < 3:
                continue
            yrs = [year_pat.search(text(c)).group(1) if year_pat.search(text(c)) else None for c in cells[1:]]
            valid = [y for y in yrs if y and any(d.startswith(y) for d in dates)]
            if len(valid) >= 2:
                header_idx = idx
                header_years = yrs
                break
        if header_idx is None:
            continue

        data_start = header_idx + 1
        if data_start < len(rows):
            nxt = [text(c) for c in rows[data_start].find_all(['th','td'])]
            if all(re.fullmatch(r'[£€\$]+', cell) or not cell for cell in nxt):
                data_start += 1

        facts = []
        for tr in rows[data_start:]:
            cells = tr.find_all(['th','td'])
            if len(cells) < 2:
                continue
            label = text(cells[0])
            if not re.search(r'[A-Za-z]', label):
                continue
            tag = re.sub(r'\W+', '_', label)
            for col, yr in enumerate(header_years, start=1):
                if not yr or col >= len(cells):
                    continue
                raw = text(cells[col])
                clean = raw.replace(',', '').replace('(', '-').replace(')', '').strip()
                if not re.fullmatch(r'-?\d+(\.\d+)?', clean):
                    continue
                period_end = next((d for d in dates if d.startswith(yr)), None)
                if not period_end:
                    continue
                facts.append({
                    'company_number': company,
                    'period_end':     period_end,
                    'metric_tag':     tag,
                    'metric_label':   label,
                    'value':          clean,
                    'unit':           ''
                })
        if facts:
            return facts

    return []


def parse_instance_fileobj(f):

    f.seek(0)
    company, _ = extract_company_and_dates(f)
    f.seek(0)

    contexts = {}
    for _, ctx in etree.iterparse(f, events=('end',), tag=f"{{{NS['xbrli']}}}context"):
        cid = ctx.get('id')
        end = ctx.find('.//xbrli:endDate', namespaces=NS)
        if cid and end is not None and end.text:
            contexts[cid] = end.text.strip()
        ctx.clear()
    f.seek(0)

    facts = []
    for _, fact in etree.iterparse(f, events=('end',), tag=f"{{{NS['ix']}}}nonFraction"):
        name   = fact.get('name')
        ctxref = fact.get('contextRef')
        val    = (fact.text or '').strip().replace(',', '')
        if not name or ctxref not in contexts:
            fact.clear()
            continue
        try:
            float(val)
        except:
            fact.clear()
            continue

        facts.append({
            'company_number': company,
            'period_end':     contexts[ctxref],
            'metric_tag':     name.split(':')[-1],
            'metric_label':   fact.get('{http://www.w3.org/1999/xhtml}title','').strip(),
            'value':          val,
            'unit':           fact.get('unitRef','').strip()
        })
        fact.clear()

    f.seek(0)
    return facts


def main():
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(ZIP_PATH, 'r') as z, OUT_CSV.open('w', newline='', encoding='utf-8') as out_f:
        writer = csv.DictWriter(out_f, fieldnames=[
            'company_number','period_end','metric_tag',
            'metric_label','value','unit'
        ])
        writer.writeheader()

        # process all HTML/XHTML files
        infos = sorted(
            [i for i in z.infolist() if i.filename.lower().endswith(('.html','.xhtml'))],
            key=lambda i: i.filename
        )

        for info in tqdm(infos, desc="Files"):
            data = z.read(info)
            buf = io.BytesIO(data)

            company, dates = extract_company_and_dates(buf)

            rows = []
            rows.extend(parse_instance_fileobj(buf))

            buf.seek(0)
            html_str = buf.read().decode('utf-8', errors='ignore')
            df = find_balance_sheet_df(html_str)
            if df is not None:
                rows.extend(process_table(df, company, dates))
            else:
                buf.seek(0)
                rows.extend(extract_table_facts(buf))

            buf.seek(0)
            # optional: generic HTML fallback if needed
            # rows.extend(extract_generic_html_facts(buf, company, dates))

            seen = set()
            for r in rows:
                key = (r['company_number'], r['period_end'], r['metric_tag'], r['value'])
                if key in seen:
                    continue
                seen.add(key)
                writer.writerow(r)

    print(f"Done! Output written to: {OUT_CSV}")

if __name__ == '__main__':
    main()
