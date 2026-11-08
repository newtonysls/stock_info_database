#!/usr/bin/env python3
"""Build one markdown profile per currently listed A-share.

Facts are copied from East Money public endpoints. Fields that are null or
absent are omitted. The script does not fill gaps from memory or inference.
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / "data" / "cache"
STOCKS = ROOT / "stocks"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
}

QUOTE_FS = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"
QUOTE_FIELDS = "f12,f13,f14,f2,f3,f9,f20,f21,f23,f26,f38,f39,f100,f102,f103"
DATACENTER = "https://datacenter.eastmoney.com/securities/api/data/get"

# Report periods kept for "latest available". Newest first.
REPORT_DATES = [
    "2026-09-30",
    "2026-06-30",
    "2026-03-31",
    "2025-12-31",
    "2025-09-30",
    "2025-06-30",
]
HOLDER_DATES = [
    "2026-09-30",
    "2026-06-30",
    "2026-03-31",
    "2025-12-31",
    "2025-09-30",
]
MAINOP_DATES = [
    "2026-09-30",
    "2026-06-30",
    "2026-03-31",
    "2025-12-31",
    "2025-09-30",
    "2025-06-30",
]

MAINOP_TYPE = {"1": "按行业", "2": "按产品", "3": "按地区"}

# (field, label, kind) kind: yuan, percent, number, text
FINANCE_FIELDS = [
    ("TOTALOPERATEREVE", "营业总收入", "yuan"),
    ("TOTALOPERATEREVETZ", "营业总收入同比", "percent"),
    ("OPERATE_INCOME_PK", "营业收入", "yuan"),
    ("OPERATE_PROFIT_PK", "营业利润", "yuan"),
    ("PARENTNETPROFIT", "归属于上市公司股东的净利润", "yuan"),
    ("PARENTNETPROFITTZ", "归母净利润同比", "percent"),
    ("KCFJCXSYJLR", "扣除非经常性损益后的归母净利润", "yuan"),
    ("KCFJCXSYJLRTZ", "扣非归母净利润同比", "percent"),
    ("EPSJB", "基本每股收益", "per_share"),
    ("BPS", "每股净资产", "per_share"),
    ("MGJYXJJE", "每股经营现金流", "per_share"),
    ("ROEJQ", "加权平均净资产收益率", "percent"),
    ("XSMLL", "销售毛利率", "percent"),
    ("XSJLL", "销售净利率", "percent"),
    ("ZCFZL", "资产负债率", "percent"),
    ("TOTAL_ASSETS_PK", "总资产", "yuan"),
    ("TOTAL_EQUITY_PK", "股东权益合计", "yuan"),
    ("LIABILITY", "负债合计", "yuan"),
    ("NETCASH_OPERATE_PK", "经营活动现金流量净额", "yuan"),
    ("NETCASH_INVEST_PK", "投资活动现金流量净额", "yuan"),
    ("NETCASH_FINANCE_PK", "筹资活动现金流量净额", "yuan"),
    ("TOTALDEPOSITS", "存款总额", "yuan"),
    ("GROSSLOANS", "贷款总额", "yuan"),
    ("LOAN_ADVANCES", "发放贷款及垫款", "yuan"),
    ("NON_PERFORMING_LOAN", "不良贷款", "yuan"),
    ("NONPERLOAN", "不良贷款率", "percent"),
    ("LOAN_PROVISION_RATIO", "贷款拨备率", "percent"),
    ("NET_INTEREST_MARGIN", "净息差", "percent"),
    ("NET_INTEREST_SPREAD", "净利差", "percent"),
    ("FIRST_ADEQUACY_RATIO", "核心一级资本充足率", "percent"),
    ("EARNED_PREMIUM", "已赚保费", "yuan"),
    ("COMPENSATE_EXPENSE", "赔付支出", "yuan"),
    ("SOLVENCY_AR", "偿付能力充足率", "percent"),
    ("SURRENDER_RATE_LIFE", "退保率", "percent"),
]

SUPPLY_KEYWORDS = ("供应商", "客户", "经销", "上游", "下游", "采购", "销售渠道")


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def decode_response(raw: bytes, content_encoding: str) -> dict:
    encoding = (content_encoding or "").lower()
    if "gzip" in encoding or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    elif "deflate" in encoding:
        try:
            raw = zlib.decompress(raw)
        except zlib.error:
            raw = zlib.decompress(raw, -zlib.MAX_WBITS)
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    if not raw:
        raise ValueError("empty body")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("gbk")
    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError("response is not an object")
    return data


def fetch_json(url: str, timeout: int = 70, retries: int = 6) -> dict:
    last: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                encoding = resp.headers.get("Content-Encoding", "")
            return decode_response(raw, encoding)
        except Exception as exc:  # noqa: BLE001 - retry any transport/parse error
            last = exc
            time.sleep(min(30, 0.6 * (2**attempt)))
    assert last is not None
    raise last


def datacenter_url(report: str, columns: str, page: int, page_size: int, filt: str, sort: str, desc: bool) -> str:
    params = {
        "type": report,
        "sty": columns,
        "p": str(page),
        "ps": str(page_size),
        "sr": "-1" if desc else "1",
        "st": sort,
        "source": "HSF10",
        "client": "PC",
    }
    if filt:
        params["filter"] = filt
    return DATACENTER + "?" + urllib.parse.urlencode(params)


def load_pages(cache_name: str, report: str, columns: str, filt: str, sort: str, desc: bool = False) -> list[dict]:
    folder = CACHE / cache_name
    folder.mkdir(parents=True, exist_ok=True)
    meta_path = folder / "_meta.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        rows: list[dict] = []
        for i in range(1, meta["pages"] + 1):
            page_path = folder / f"{i:04d}.json"
            rows.extend(json.loads(page_path.read_text(encoding="utf-8")))
        print(f"[cache] {cache_name} rows={len(rows)}", flush=True)
        return rows

    first = None
    for attempt in range(8):
        first = fetch_json(datacenter_url(report, columns, 1, 500, filt, sort, desc))
        message = str(first.get("message") or "")
        if any(word in message for word in ("繁忙", "超时", "稍后", "频繁", "系统异常")):
            time.sleep(min(25, 1.5 * (attempt + 1)))
            continue
        break
    else:
        raise RuntimeError(f"{cache_name} page 1 busy: {(first or {}).get('message')}")
    result = (first or {}).get("result") or {}
    if not result or not result.get("count"):
        message = str((first or {}).get("message") or "")
        if any(word in message for word in ("繁忙", "超时", "稍后", "频繁", "系统异常")):
            raise RuntimeError(f"{cache_name} page 1 busy: {message}")
        meta_path.write_text(
            json.dumps({"pages": 0, "count": 0, "fetched_at": now_iso()}, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"[fetch] {cache_name} empty msg={message}", flush=True)
        return []
    count = int(result["count"])
    pages = (count + 499) // 500
    (folder / "0001.json").write_text(
        json.dumps(result.get("data") or [], ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"[fetch] {cache_name} count={count} pages={pages}", flush=True)

    def pull(page: int) -> tuple[int, list[dict]]:
        last_msg = ""
        for attempt in range(8):
            payload = fetch_json(datacenter_url(report, columns, page, 500, filt, sort, desc))
            data = (payload.get("result") or {}).get("data") or []
            last_msg = str(payload.get("message") or "")
            if data:
                return page, data
            time.sleep(min(25, 1.5 * (attempt + 1)))
        raise RuntimeError(f"{cache_name} page {page} empty: {last_msg}")

    pending = [p for p in range(2, pages + 1) if not (folder / f"{p:04d}.json").exists()]
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(pull, page): page for page in pending}
        done = 1
        for fut in as_completed(futures):
            page, data = fut.result()
            (folder / f"{page:04d}.json").write_text(
                json.dumps(data, ensure_ascii=False),
                encoding="utf-8",
            )
            done += 1
            if done % 25 == 0 or done == pages:
                print(f"[fetch] {cache_name} {done}/{pages}", flush=True)

    rows = []
    for i in range(1, pages + 1):
        rows.extend(json.loads((folder / f"{i:04d}.json").read_text(encoding="utf-8")))
    meta_path.write_text(
        json.dumps({"pages": pages, "count": count, "fetched_at": now_iso(), "rows": len(rows)}, ensure_ascii=False),
        encoding="utf-8",
    )
    if len(rows) < count * 0.98:
        raise RuntimeError(f"{cache_name} incomplete: got {len(rows)} of {count}")
    return rows


def fetch_quotes() -> list[dict]:
    folder = CACHE / "quotes"
    folder.mkdir(parents=True, exist_ok=True)
    meta_path = folder / "_meta.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        rows: list[dict] = []
        for i in range(1, meta["pages"] + 1):
            rows.extend(json.loads((folder / f"{i:04d}.json").read_text(encoding="utf-8")))
        print(f"[cache] quotes rows={len(rows)}", flush=True)
        return rows

    def page_url(page: int) -> str:
        fs = urllib.parse.quote(QUOTE_FS, safe=":,+")
        return (
            "https://push2delay.eastmoney.com/api/qt/clist/get"
            f"?pn={page}&pz=100&po=1&np=1&fltt=2&invt=2&fid=f12"
            f"&fs={fs}&fields={QUOTE_FIELDS}"
        )

    first = fetch_json(page_url(1))
    total = int((first.get("data") or {}).get("total") or 0)
    if total < 1000:
        raise RuntimeError(f"quote total looks wrong: {total}")
    pages = (total + 99) // 100
    diff = (first.get("data") or {}).get("diff") or []
    (folder / "0001.json").write_text(json.dumps(diff, ensure_ascii=False), encoding="utf-8")
    print(f"[fetch] quotes total={total} pages={pages}", flush=True)

    def pull(page: int) -> tuple[int, list[dict]]:
        payload = fetch_json(page_url(page))
        data = (payload.get("data") or {}).get("diff") or []
        if len(data) < 50 and page != pages:
            raise RuntimeError(f"quote page {page} short: {len(data)}")
        return page, data

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(pull, page): page for page in range(2, pages + 1)}
        done = 1
        for fut in as_completed(futures):
            page, data = fut.result()
            (folder / f"{page:04d}.json").write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            done += 1
            if done % 20 == 0 or done == pages:
                print(f"[fetch] quotes {done}/{pages}", flush=True)

    rows = []
    for i in range(1, pages + 1):
        rows.extend(json.loads((folder / f"{i:04d}.json").read_text(encoding="utf-8")))
    uniq = {str(row["f12"]): row for row in rows}
    if len(uniq) < total * 0.99:
        raise RuntimeError(f"quotes incomplete: unique {len(uniq)} of {total}")
    meta_path.write_text(
        json.dumps({"pages": pages, "count": total, "unique": len(uniq), "fetched_at": now_iso()}, ensure_ascii=False),
        encoding="utf-8",
    )
    return list(uniq.values())


def market_of(code: str, market_flag) -> tuple[str, str]:
    flag = str(market_flag)
    if flag == "1":
        return "SH", "上海证券交易所"
    if code.startswith(("92", "83", "87", "43", "82", "88", "4")):
        return "BJ", "北京证券交易所"
    return "SZ", "深圳证券交易所"


def clean_text(value) -> str:
    if value is None:
        return ""
    text = str(value).replace("\r\n", "\n").replace("\r", "\n").replace("\u3000", " ")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def is_missing(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() in {"", "-", "--", "—", "null", "None"}:
        return True
    return False


def is_listed_quote(row: dict) -> bool:
    """Keep currently listed A-shares. Drop delisted, transfer-board and not-yet-listed rows."""
    name = str(row.get("f14") or "")
    if "退市" in name or name.startswith("PT") or "定转" in name or name.endswith("退"):
        return False
    listed = str(row.get("f26") or "")
    if len(listed) != 8 or not listed.isdigit():
        return False
    price = row.get("f2")
    no_price = is_missing(price)
    if not no_price:
        try:
            no_price = float(price) <= 0
        except (TypeError, ValueError):
            no_price = True
    industry = str(row.get("f100") or "-")
    if no_price and industry in {"-", "", "--"}:
        return False
    return True


def date_only(value) -> str:
    text = clean_text(value)
    return text[:10] if len(text) >= 10 else text


def yaml_quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ") + '"'


def fmt_num(value, digits: int = 2) -> str | None:
    if is_missing(value):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return clean_text(value)
    text = f"{number:,.{digits}f}"
    if text.startswith("-") and number == 0:
        text = text.lstrip("-")
    return text


def fmt_int(value) -> str | None:
    if is_missing(value):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return clean_text(value)
    return f"{number:,.0f}"


def fmt_yuan(value) -> str | None:
    if is_missing(value):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return clean_text(value)
    yi = number / 100000000
    return f"{yi:,.2f} 亿元（原始值 {number:,.2f} 元）"


def fmt_percent(value) -> str | None:
    shown = fmt_num(value, 2)
    return None if shown is None else f"{shown}%"


def fmt_ratio_percent(value) -> str | None:
    if is_missing(value):
        return None
    try:
        number = float(value) * 100
    except (TypeError, ValueError):
        return clean_text(value)
    return f"{number:.2f}%"


def cell(value: str | None) -> str:
    if value is None:
        return ""
    return value.replace("|", "\\|").replace("\n", " ")


def index_by_code(rows: list[dict], field: str = "SECURITY_CODE") -> dict[str, dict]:
    out: dict[str, dict] = {}
    for row in rows:
        code = str(row.get(field) or "").strip()
        if code:
            out[code] = row
    return out


def group_rows(rows: list[dict], field: str = "SECURITY_CODE") -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for row in rows:
        code = str(row.get(field) or "").strip()
        if code:
            out.setdefault(code, []).append(row)
    return out


def latest_dated(rows: list[dict], date_field: str) -> list[dict]:
    if not rows:
        return []
    best = max(date_only(row.get(date_field)) for row in rows)
    picked = [row for row in rows if date_only(row.get(date_field)) == best]
    return picked


def description_of(profile: dict | None, quote: dict) -> str:
    source = ""
    if profile:
        source = clean_text(profile.get("ORG_PROFILE")) or clean_text(profile.get("MAIN_BUSINESS"))
    source = re.sub(r"\s+", " ", source).strip()
    if source:
        if len(source) > 180:
            cut = source[:180]
            pivot = max(cut.rfind("。"), cut.rfind("；"), cut.rfind("，"))
            source = (cut[: pivot + 1] if pivot >= 40 else cut.rstrip()) + "…"
        return source
    name = clean_text(quote.get("f14")) or "名称未返回"
    code = str(quote.get("f12"))
    industry = clean_text(quote.get("f100"))
    prefix, market = market_of(code, quote.get("f13"))
    bits = [f"{name}（{code}.{prefix}）", f"交易场所：{market}"]
    if industry:
        bits.append(f"行情行业：{industry}")
    return "，".join(bits)


def quote_lines(quote: dict, profile: dict | None) -> list[str]:
    code = str(quote["f12"])
    prefix, market = market_of(code, quote.get("f13"))
    lines = [
        f"- 股票代码：{code}",
        f"- 证券简称：{clean_text(quote.get('f14')) or '接口未返回'}",
        f"- 交易场所：{market}（行情市场标记 f13={quote.get('f13')}，证券代码后缀 {prefix}）",
    ]
    if profile and not is_missing(profile.get("SECURITY_TYPE")):
        lines.append(f"- 证券类别：{clean_text(profile.get('SECURITY_TYPE'))}")
    if profile and not is_missing(profile.get("LISTING_DATE")):
        lines.append(f"- 上市日期：{date_only(profile.get('LISTING_DATE'))}（公司资料接口）")
    elif not is_missing(quote.get("f26")):
        raw = str(quote.get("f26"))
        shown = f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}" if len(raw) == 8 else raw
        lines.append(f"- 上市日期：{shown}（行情列表字段 f26）")
    if not is_missing(quote.get("f100")):
        lines.append(f"- 行情行业：{clean_text(quote.get('f100'))}")
    if not is_missing(quote.get("f102")):
        lines.append(f"- 地域板块：{clean_text(quote.get('f102'))}")
    price = fmt_num(quote.get("f2"), 2)
    if price:
        lines.append(f"- 采集时最新价：{price} 元")
    change = fmt_percent(quote.get("f3"))
    if change:
        lines.append(f"- 采集时涨跌幅：{change}")
    cap = fmt_yuan(quote.get("f20"))
    if cap:
        lines.append(f"- 采集时总市值：{cap}")
    float_cap = fmt_yuan(quote.get("f21"))
    if float_cap:
        lines.append(f"- 采集时流通市值：{float_cap}")
    pe = fmt_num(quote.get("f9"), 2)
    if pe:
        lines.append(f"- 市盈率（动态，行情字段 f9）：{pe}")
    pb = fmt_num(quote.get("f23"), 2)
    if pb:
        lines.append(f"- 市净率（行情字段 f23）：{pb}")
    total_share = fmt_int(quote.get("f38"))
    if total_share:
        lines.append(f"- 总股本：{total_share} 股")
    float_share = fmt_int(quote.get("f39"))
    if float_share:
        lines.append(f"- 流通股本：{float_share} 股")
    concepts = clean_text(quote.get("f103"))
    if concepts:
        lines.append(f"- 行情列表概念标签：{concepts}")
        lines.append("  概念标签是行情终端给出的分类，不是公司公告原文。")
    lines.append("- 行情数字只对应该次列表接口的返回，不是重新计算的估值。")
    return lines


def profile_lines(profile: dict | None) -> list[str]:
    if not profile:
        return ["公司概况接口未返回该代码。本文不另写公司沿革或业务判断。"]
    mapping = [
        ("ORG_NAME", "公司全称"),
        ("ORG_NAME_EN", "英文名称"),
        ("FORMERNAME", "曾用名"),
        ("FOUND_DATE", "成立日期"),
        ("LISTING_DATE", "上市日期"),
        ("REG_CAPITAL", "注册资本（万元，字段 REG_CAPITAL 原值）"),
        ("EMP_NUM", "员工人数"),
        ("TATOLNUMBER", "管理人员人数"),
        ("LEGAL_PERSON", "法定代表人"),
        ("CHAIRMAN", "董事长"),
        ("PRESIDENT", "总经理"),
        ("SECRETARY", "董事会秘书"),
        ("INDEDIRECTORS", "独立董事"),
        ("ACTUAL_HOLDER", "资料接口中的实控人字段"),
        ("PROVINCE", "省份"),
        ("REG_ADDRESS", "注册地址"),
        ("ADDRESS", "办公地址"),
        ("ADDRESS_POSTCODE", "邮政编码"),
        ("ORG_TEL", "电话"),
        ("ORG_FAX", "传真"),
        ("ORG_EMAIL", "邮箱"),
        ("ORG_WEB", "公司网站"),
        ("REG_NUM", "统一社会信用代码/注册号"),
        ("LAW_FIRM", "律师事务所"),
        ("ACCOUNTFIRM_NAME", "会计师事务所"),
        ("EM2016", "东财行业"),
        ("INDUSTRYCSRC1", "证监会行业"),
        ("BOARD_NAME_LEVEL", "板块层级"),
        ("CURRENCY", "交易币种"),
    ]
    lines = []
    for key, label in mapping:
        value = profile.get(key)
        if is_missing(value):
            continue
        shown = date_only(value) if key in {"FOUND_DATE", "LISTING_DATE"} else clean_text(value)
        if key == "REG_CAPITAL":
            shown = fmt_num(value, 4) or shown
        if key == "EMP_NUM" or key == "TATOLNUMBER":
            shown = fmt_int(value) or shown
        lines.append(f"- {label}：{shown}")
    scope = clean_text(profile.get("BUSINESS_SCOPE"))
    main_biz = clean_text(profile.get("MAIN_BUSINESS"))
    intro = clean_text(profile.get("ORG_PROFILE"))
    if main_biz:
        lines.append("")
        lines.append("主营业务（字段 MAIN_BUSINESS，原文）：")
        lines.append("")
        lines.append(main_biz)
    if scope:
        lines.append("")
        lines.append("经营范围（字段 BUSINESS_SCOPE，原文）：")
        lines.append("")
        lines.append(scope)
    if intro:
        lines.append("")
        lines.append("公司简介（字段 ORG_PROFILE，原文）：")
        lines.append("")
        lines.append(intro)
    if not lines:
        lines.append("公司概况接口有记录，但简介、经营范围和主要身份字段均为空。")
    return lines


def finance_lines(rows: list[dict]) -> list[str]:
    if not rows:
        return ["主要财务指标接口在已请求的报告期中没有返回该代码。本文不估算财务数据。"]
    latest_rows = latest_dated(rows, "REPORT_DATE")
    row = sorted(latest_rows, key=lambda item: date_only(item.get("NOTICE_DATE") or ""), reverse=True)[0]
    lines = [
        f"- 报告期：{clean_text(row.get('REPORT_DATE_NAME')) or date_only(row.get('REPORT_DATE'))}",
        f"- 报告截止日：{date_only(row.get('REPORT_DATE'))}",
    ]
    if not is_missing(row.get("NOTICE_DATE")):
        lines.append(f"- 公告日期：{date_only(row.get('NOTICE_DATE'))}")
    if not is_missing(row.get("ORG_TYPE")):
        lines.append(f"- 报表类型：{clean_text(row.get('ORG_TYPE'))}")
    if not is_missing(row.get("CURRENCY")):
        lines.append(f"- 币种：{clean_text(row.get('CURRENCY'))}")
    lines.append("- 金额类字段原始单位为元，下表同时给出换算后的亿元，保留两位小数。百分比字段按接口原值加百分号，不是由本文重算。")
    lines.append("")
    lines.append("| 项目 | 数值 |")
    lines.append("| --- | --- |")
    any_row = False
    for key, label, kind in FINANCE_FIELDS:
        value = row.get(key)
        if is_missing(value):
            continue
        if kind == "yuan":
            shown = fmt_yuan(value)
        elif kind == "percent":
            shown = fmt_percent(value)
        elif kind == "per_share":
            shown = fmt_num(value, 4)
            shown = None if shown is None else f"{shown} 元"
        else:
            shown = fmt_num(value, 2)
        if shown:
            lines.append(f"| {label} | {cell(shown)} |")
            any_row = True
    if not any_row:
        lines.append("| （所选字段均为空） |  |")
    older = sorted({date_only(item.get("REPORT_DATE")) for item in rows if date_only(item.get("REPORT_DATE"))})
    if len(older) > 1:
        lines.append("")
        lines.append("同一接口在下列报告期也有记录，正文只展开最新一期：")
        lines.append("、".join(older) + "。")
    return lines


def mainop_lines(rows: list[dict]) -> list[str]:
    if not rows:
        return ["主营构成接口在已请求的报告期中没有返回该代码。"]
    latest = latest_dated(rows, "REPORT_DATE")
    report_date = date_only(latest[0].get("REPORT_DATE"))
    lines = [
        f"报告期：{report_date}。收入占比、毛利率在接口中是小数，下表已乘以 100 后显示。",
        "",
    ]
    grouped: dict[str, list[dict]] = {}
    for row in latest:
        grouped.setdefault(str(row.get("MAINOP_TYPE")), []).append(row)
    for type_code, items in sorted(grouped.items(), key=lambda kv: kv[0]):
        title = MAINOP_TYPE.get(type_code, f"分类代码 {type_code}")
        lines.append(f"### {title}")
        lines.append("")
        lines.append("| 项目 | 主营收入 | 收入比例 | 毛利率 |")
        lines.append("| --- | --- | --- | --- |")
        items = sorted(items, key=lambda item: float(item.get("MBI_RATIO") or 0), reverse=True)
        for item in items:
            income = fmt_yuan(item.get("MAIN_BUSINESS_INCOME")) or ""
            ratio = fmt_ratio_percent(item.get("MBI_RATIO")) or ""
            gross = fmt_ratio_percent(item.get("GROSS_RPOFIT_RATIO")) or ""
            lines.append(
                f"| {cell(clean_text(item.get('ITEM_NAME')))} | {cell(income)} | {cell(ratio)} | {cell(gross)} |"
            )
        lines.append("")
    return lines


def supply_lines(mainop: list[dict], review_text: str) -> list[str]:
    lines = [
        "本节只使用两类已返回材料：主营构成里的产品、行业和地区收入，以及经营评述里逐句原样摘出的相关句子。",
        "已查询的结构化接口没有前五名客户、前五名供应商名单。本文不根据行业或公司名称补写上游供应商、下游客户。",
        "",
    ]
    if mainop:
        latest = latest_dated(mainop, "REPORT_DATE")
        report_date = date_only(latest[0].get("REPORT_DATE"))
        lines.append(f"收入结构来自 {report_date} 主营构成。")
        lines.append("")
        for type_code, title in (("2", "按产品"), ("1", "按行业"), ("3", "按地区")):
            items = [row for row in latest if str(row.get("MAINOP_TYPE")) == type_code]
            if not items:
                continue
            lines.append(f"- {title}：")
            items = sorted(items, key=lambda item: float(item.get("MBI_RATIO") or 0), reverse=True)
            for item in items:
                ratio = fmt_ratio_percent(item.get("MBI_RATIO")) or "比例未返回"
                lines.append(f"  - {clean_text(item.get('ITEM_NAME'))}：收入比例 {ratio}")
        lines.append("")
    else:
        lines.append("主营构成未返回，因此没有可列示的产品或地区收入结构。")
        lines.append("")
    sentences = []
    if review_text:
        parts = re.split(r"(?<=[。！？])|\n+", review_text)
        for part in parts:
            piece = part.strip()
            if piece and any(word in piece for word in SUPPLY_KEYWORDS):
                sentences.append(piece)
    if sentences:
        lines.append("下列句子从经营评述原文逐句抄出，只保留含有供应链、客户、经销或采购字样的句子：")
        lines.append("")
        for sentence in sentences:
            lines.append(f"> {sentence}")
            lines.append("")
    else:
        lines.append("经营评述原文中没有检索到“供应商、客户、经销、上游、下游、采购、销售渠道”。没有另写链路。")
    return lines


def relation_lines(rows: list[dict]) -> list[str]:
    if not rows:
        return ["控股与实控关系接口未返回该代码。"]
    lines = []
    for row in rows:
        relation = clean_text(row.get("RELATED_RELATION")) or "关系未标注"
        name = clean_text(row.get("HOLDER_NAME")) or "名称未返回"
        ratio = fmt_percent(row.get("HOLD_RATIO"))
        end = date_only(row.get("END_DATE"))
        extra = []
        if ratio:
            extra.append(f"持股比例 {ratio}")
        if end:
            extra.append(f"截止日期 {end}")
        suffix = f"（{'，'.join(extra)}）" if extra else ""
        lines.append(f"- {relation}：{name}{suffix}")
    return lines


def holder_lines(rows: list[dict]) -> list[str]:
    if not rows:
        return ["前十大股东接口在已请求的截止日期中没有返回该代码。"]
    latest = latest_dated(rows, "END_DATE")
    end = date_only(latest[0].get("END_DATE"))
    latest = sorted(latest, key=lambda item: int(float(item.get("HOLDER_RANK") or 0)))
    lines = [
        f"截止日期：{end}。持股比例按接口原值显示并加百分号。",
        "",
        "| 排名 | 股东名称 | 持股数量（股） | 持股比例 | 持股变动 | 变动说明 |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in latest:
        lines.append(
            "| {rank} | {name} | {num} | {ratio} | {change} | {state} |".format(
                rank=cell(clean_text(row.get("HOLDER_RANK"))),
                name=cell(clean_text(row.get("HOLDER_NAME"))),
                num=cell(fmt_int(row.get("HOLD_NUM")) or ""),
                ratio=cell(fmt_percent(row.get("HOLD_NUM_RATIO")) or ""),
                change=cell(fmt_int(row.get("HOLD_NUM_CHANGE")) or ""),
                state=cell(clean_text(row.get("HOLDER_STATE"))),
            )
        )
    return lines


def holding_lines(rows: list[dict]) -> list[str]:
    if not rows:
        return ["参股控股接口未返回该代码。"]
    report_dates = sorted({date_only(row.get("REPORT_DATE")) for row in rows if date_only(row.get("REPORT_DATE"))})
    lines = [
        "接口只提供最近一次披露快照。注册资本、投资额、净利润若接口已带单位，则按原文字写入，不再换算。",
    ]
    if report_dates:
        lines.append("报告期：" + "、".join(report_dates) + "。")
    lines.append("")
    lines.append("| 公司 | 关系 | 持股比例 | 注册资本 | 投资额 | 净利润 | 主营产品 |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- |")
    rows = sorted(rows, key=lambda item: float(item.get("ORG_HOLD_RATIO") or -1), reverse=True)
    for row in rows:
        ratio = fmt_percent(row.get("ORG_HOLD_RATIO")) or ""
        lines.append(
            "| {name} | {kind} | {ratio} | {reg} | {invest} | {profit} | {product} |".format(
                name=cell(clean_text(row.get("HOLD_ORG_NAME"))),
                kind=cell(clean_text(row.get("HOLD_TYPE"))),
                ratio=cell(ratio),
                reg=cell(clean_text(row.get("REG_CAPITAL"))),
                invest=cell(clean_text(row.get("ACTUAL_INVEST_AMT"))),
                profit=cell(clean_text(row.get("NETPROFIT"))),
                product=cell(clean_text(row.get("MAIN_PRODUCTS"))),
            )
        )
    return lines


def truncate_note(label: str, rows: list) -> str | None:
    if isinstance(rows, list) and len(rows) >= 100:
        return f"{label}返回 {len(rows)} 条，与该接口常见单次上限相同，可能不是完整历史。"
    return None


def event_lines(events: dict | None) -> list[str]:
    if events is None:
        return ["公司大事接口没有成功返回，风险栏目留空，不用其他材料补写。"]
    if events.get("error"):
        return [f"公司大事接口请求失败：{events.get('error')}。失败后不补写风险内容。"]
    lines = [
        "本节只收录公司大事接口返回的违规处理、股权质押、对外担保和诉讼仲裁。",
        "金额字段本身经常不带单位，一律按原值列出，不换算。返回条数达到 100 时，按该接口的常见上限处理，不视为完整历史。",
        "",
    ]
    sections = [
        ("wgcl", "违规处理", render_violation),
        ("gqzy", "股权质押", render_pledge),
        ("dwdb", "对外担保", render_guarantee),
        ("sszc", "诉讼仲裁", render_lawsuit),
    ]
    any_record = False
    for key, title, renderer in sections:
        rows = events.get(key) or []
        lines.append(f"### {title}")
        lines.append("")
        note = truncate_note(title, rows)
        if note:
            lines.append(note)
            lines.append("")
        if not rows:
            lines.append("接口未返回记录。")
            lines.append("")
            continue
        any_record = True
        lines.extend(renderer(rows))
        lines.append("")
    if not any_record:
        lines.append("以上四类在该接口中都没有记录。这只说明接口没有返回，不表示公司不存在其他风险。")
    return lines


def render_violation(rows: list[dict]) -> list[str]:
    rows = sorted(rows, key=lambda item: date_only(item.get("NOTICE_DATE")), reverse=True)
    lines = []
    for row in rows:
        lines.append(f"- 公告日期：{date_only(row.get('NOTICE_DATE')) or '未返回'}")
        for key, label in (
            ("SOLVE_ORG", "处理机构"),
            ("PUNISH_OBJECT", "处罚对象"),
            ("VIOLATE_TYPE", "违规类型"),
            ("PUNISH_TYPE", "处罚类型"),
            ("PUNISH_AMT", "处罚金额（原值）"),
            ("PUNISH_STEP", "处理措施"),
        ):
            if not is_missing(row.get(key)):
                lines.append(f"  - {label}：{clean_text(row.get(key))}")
        behaviour = clean_text(row.get("VIOLATE_BEHAVIOUR"))
        if behaviour:
            lines.append("  - 违规行为原文：")
            lines.append("")
            lines.append(indent_block(behaviour))
            lines.append("")
    return lines


def render_pledge(rows: list[dict]) -> list[str]:
    rows = sorted(rows, key=lambda item: date_only(item.get("NOTICE_DATE")), reverse=True)
    lines = [
        "| 公告日期 | 股东 | 质权人 | 质押股数 | 占总股本(%) | 占所持股份(%) | 起始日 | 解押日 | 实际解押日 | 是否质押式 |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            "| {notice} | {holder} | {org} | {num} | {tsr} | {hold} | {start} | {end} | {actual} | {kind} |".format(
                notice=cell(date_only(row.get("NOTICE_DATE"))),
                holder=cell(clean_text(row.get("HOLDER_NAME"))),
                org=cell(clean_text(row.get("PF_ORG"))),
                num=cell(fmt_int(row.get("PF_NUM")) or ""),
                tsr=cell(fmt_num(row.get("PF_TSR"), 2) or ""),
                hold=cell(fmt_num(row.get("PF_HOLD_RATIO"), 2) or ""),
                start=cell(date_only(row.get("PF_START_DATE"))),
                end=cell(date_only(row.get("UNFREEZE_DATE"))),
                actual=cell(date_only(row.get("ACTUAL_UNFREEZE_DATE"))),
                kind=cell(clean_text(row.get("IS_PFTYPE"))),
            )
        )
    lines.append("")
    lines.append("占总股本、占所持股份两列对应字段 PF_TSR、PF_HOLD_RATIO，按百分比原值列出。")
    return lines


def render_guarantee(rows: list[dict]) -> list[str]:
    rows = sorted(rows, key=lambda item: date_only(item.get("NOTICE_DATE")), reverse=True)
    lines = []
    for row in rows:
        lines.append(f"- 公告日期：{date_only(row.get('NOTICE_DATE')) or '未返回'}")
        for key, label in (
            ("GUAR_NAME", "担保方"),
            ("GUARANTEED_NAME", "被担保方"),
            ("GUARANTEE_WAY", "担保方式"),
            ("GUARANTEE_AMT", "担保金额（原值）"),
            ("CURRENCY", "币种"),
            ("IS_PERFORM", "是否履行"),
            ("IS_RELATED_TRADE", "是否关联交易"),
            ("REPORT_DATE", "报告期"),
            ("GUARANTEE_START_DATE", "起始日"),
            ("GUARANTEE_END_DATE", "到期日"),
        ):
            if not is_missing(row.get(key)):
                shown = date_only(row.get(key)) if "DATE" in key else clean_text(row.get(key))
                lines.append(f"  - {label}：{shown}")
        explain = clean_text(row.get("GUAR_EVENT_EXPLAIN"))
        if explain:
            lines.append("  - 事件说明原文：")
            lines.append("")
            lines.append(indent_block(explain))
            lines.append("")
    return lines


def render_lawsuit(rows: list[dict]) -> list[str]:
    rows = sorted(rows, key=lambda item: date_only(item.get("NOTICE_DATE")), reverse=True)
    lines = []
    for row in rows:
        lines.append(f"- 公告日期：{date_only(row.get('NOTICE_DATE')) or '未返回'}")
        for key, label in (
            ("CASE_NAME", "案件名称"),
            ("LITIGATION_TYPE", "类型"),
            ("PLAINTIFF", "原告/申请人"),
            ("DEFENCE", "被告/被申请人"),
            ("CASE_AMOUNT", "案件金额（原值）"),
            ("CURRENCY", "币种"),
            ("FI_ACCEPTANCE_COURT", "受理法院"),
            ("FI_PROSECUTE_DATE", "起诉日期"),
            ("FI_JUDGMENT_DATE", "判决日期"),
            ("FI_IS_APPEAL", "是否上诉"),
            ("EXECUTE_SITUATION", "执行情况"),
        ):
            if not is_missing(row.get(key)):
                shown = date_only(row.get(key)) if key.endswith("DATE") else clean_text(row.get(key))
                lines.append(f"  - {label}：{shown}")
        for key, label in (
            ("CASE_PROFILE", "案情原文"),
            ("FI_JUDGMENT_CONTENT", "判决内容原文"),
            ("SI_JUDGMENT_CONTENT", "二审内容原文"),
        ):
            text = clean_text(row.get(key))
            if text:
                lines.append(f"  - {label}：")
                lines.append("")
                lines.append(indent_block(text))
                lines.append("")
    return lines


def indent_block(text: str) -> str:
    return "\n".join(f"> {line}" if line else ">" for line in text.split("\n"))


def fetch_review(prefix: str, code: str) -> dict:
    url = (
        "https://emweb.securities.eastmoney.com/PC_HSF10/BusinessAnalysis/PageAjax"
        f"?code={prefix}{code}"
    )
    payload = fetch_json(url, timeout=40, retries=5)
    reviews = payload.get("jyps") or []
    if not reviews:
        return {"report_date": None, "text": "", "fetched_at": now_iso()}
    item = reviews[0]
    return {
        "report_date": date_only(item.get("REPORT_DATE")),
        "text": clean_text(item.get("BUSINESS_REVIEW")),
        "fetched_at": now_iso(),
    }


def fetch_events(prefix: str, code: str) -> dict:
    url = (
        "https://emweb.securities.eastmoney.com/PC_HSF10/CompanyBigNews/PageAjax"
        f"?code={prefix}{code}"
    )
    payload = fetch_json(url, timeout=40, retries=5)
    kept = {}
    for key in ("wgcl", "gqzy", "dwdb", "sszc"):
        value = payload.get(key)
        kept[key] = value if isinstance(value, list) else []
    kept["fetched_at"] = now_iso()
    return kept


def cache_needs_fetch(path: Path) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return True
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return True
    return bool(data.get("error"))


def ensure_per_stock(quotes: list[dict], workers: int) -> None:
    review_dir = CACHE / "review"
    event_dir = CACHE / "events"
    review_dir.mkdir(parents=True, exist_ok=True)
    event_dir.mkdir(parents=True, exist_ok=True)
    pending = []
    for quote in quotes:
        code = str(quote["f12"])
        if cache_needs_fetch(review_dir / f"{code}.json") or cache_needs_fetch(event_dir / f"{code}.json"):
            pending.append(quote)
    print(f"[fetch] per-stock pending={len(pending)} of {len(quotes)}", flush=True)
    if not pending:
        return

    def work(quote: dict) -> str:
        code = str(quote["f12"])
        prefix, _ = market_of(code, quote.get("f13"))
        review_path = review_dir / f"{code}.json"
        event_path = event_dir / f"{code}.json"
        if cache_needs_fetch(review_path):
            try:
                review = fetch_review(prefix, code)
            except Exception as exc:  # noqa: BLE001
                review = {"error": f"{type(exc).__name__}: {exc}", "text": "", "fetched_at": now_iso()}
            review_path.write_text(json.dumps(review, ensure_ascii=False), encoding="utf-8")
        if cache_needs_fetch(event_path):
            try:
                events = fetch_events(prefix, code)
            except Exception as exc:  # noqa: BLE001
                events = {"error": f"{type(exc).__name__}: {exc}", "fetched_at": now_iso()}
            event_path.write_text(json.dumps(events, ensure_ascii=False), encoding="utf-8")
        return code

    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(work, quote) for quote in pending]
        for fut in as_completed(futures):
            fut.result()
            done += 1
            if done % 100 == 0 or done == len(pending):
                print(f"[fetch] per-stock {done}/{len(pending)}", flush=True)


def render_one(
    quote: dict,
    profile: dict | None,
    finance: list[dict],
    mainop: list[dict],
    holders: list[dict],
    relations: list[dict],
    holdings: list[dict],
    review: dict,
    events: dict,
) -> str:
    code = str(quote["f12"])
    name = clean_text(quote.get("f14")) or code
    prefix, _ = market_of(code, quote.get("f13"))
    desc = description_of(profile, quote)
    review_text = clean_text(review.get("text"))
    review_date = clean_text(review.get("report_date"))
    lines = [
        "---",
        f"name: {yaml_quote(code)}",
        f"description: {yaml_quote(desc)}",
        "---",
        "",
        f"# {name}（{code}）",
        "",
        "本文只整理公开接口在采集时返回的字段和原文。接口没有返回的事实不补写，数字不重新推算，也不构成投资建议。",
        "",
        "## 数据来源",
        "",
        f"- 证券代码：{code}.{prefix}",
        "- 在市名单与行情：东方财富行情列表 `push2delay.eastmoney.com/api/qt/clist/get`，筛选沪深京 A 股。",
        "- 公司资料：数据中心报表 `RPT_F10_BASIC_ORGINFO`。",
        "- 主要财务指标：数据中心报表 `RPT_F10_FINANCE_MAINFINADATA`。",
        "- 主营构成：数据中心报表 `RPT_F10_FN_MAINOP`。",
        "- 前十大股东：数据中心报表 `RPT_F10_EH_HOLDERS`。",
        "- 控股股东与实际控制人：数据中心报表 `RPT_F10_EH_RELATION`。",
        "- 参股控股：数据中心报表 `RPT_F10_PUBLIC_OP_HOLDINGORG`。",
        f"- 经营评述：`emweb.securities.eastmoney.com/PC_HSF10/BusinessAnalysis/PageAjax?code={prefix}{code}`。",
        f"- 违规、质押、担保、诉讼：`emweb.securities.eastmoney.com/PC_HSF10/CompanyBigNews/PageAjax?code={prefix}{code}`。",
        "",
        "## 股票介绍",
        "",
        *quote_lines(quote, profile),
        "",
        "## 公司介绍",
        "",
        *profile_lines(profile),
        "",
        "## 财报介绍",
        "",
        *finance_lines(finance),
        "",
        "## 业务介绍",
        "",
    ]
    if mainop:
        lines.extend(mainop_lines(mainop))
    else:
        lines.append("主营构成接口在已请求的报告期中没有返回该代码。")
        lines.append("")
    if review.get("error"):
        lines.append(f"经营评述接口请求失败：{review.get('error')}。失败后不改写、不补写经营评述。")
    elif review_text:
        lines.append(f"经营评述（报告期 {review_date or '未标明'}，原文）：")
        lines.append("")
        lines.append(review_text)
    else:
        lines.append("经营评述接口未返回正文。")
    lines.extend(
        [
            "",
            "## 上下游链路介绍",
            "",
            *supply_lines(mainop, review_text),
            "",
            "## 投资、控股介绍",
            "",
            "### 控股股东与实际控制人",
            "",
            *relation_lines(relations),
            "",
            "### 前十大股东",
            "",
            *holder_lines(holders),
            "",
            "### 参股与控股公司",
            "",
            *holding_lines(holdings),
            "",
            "## 风险",
            "",
            *event_lines(events),
            "",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def load_json_rows(path: Path) -> list[dict]:
    if not path.exists():
        raise RuntimeError(f"missing cache file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def render_all(quotes: list[dict]) -> None:
    print("[render] loading tables", flush=True)
    profiles = index_by_code(
        load_pages(
            "basic",
            "RPT_F10_BASIC_ORGINFO",
            "ALL",
            '(SECURITY_TYPEE="A股")',
            "SECURITY_CODE",
            False,
        )
    )
    finance = group_rows(load_json_rows(CACHE / "finance_merged.json"))
    mainop = group_rows(load_json_rows(CACHE / "mainop_merged.json"))
    holders = group_rows(load_json_rows(CACHE / "holders_merged.json"))
    relations = group_rows(
        load_pages(
            "relation",
            "RPT_F10_EH_RELATION",
            "SECUCODE,SECURITY_CODE,HOLDER_NAME,RELATED_RELATION,HOLD_RATIO,END_DATE",
            "",
            "SECUCODE",
            False,
        )
    )
    holdings = group_rows(
        load_pages(
            "holdings",
            "RPT_F10_PUBLIC_OP_HOLDINGORG",
            "SECUCODE,SECURITY_CODE,HOLD_ORG_NAME,REG_CAPITAL,ORG_HOLD_RATIO,HOLD_TYPE,ACTUAL_INVEST_AMT,NETPROFIT,MAIN_PRODUCTS,REPORT_DATE",
            "",
            "SECUCODE",
            False,
        )
    )
    STOCKS.mkdir(parents=True, exist_ok=True)
    manifest = ["code\tname\tsecucode\tmarket"]
    for quote in sorted(quotes, key=lambda item: str(item["f12"])):
        code = str(quote["f12"])
        prefix, market = market_of(code, quote.get("f13"))
        review = json.loads((CACHE / "review" / f"{code}.json").read_text(encoding="utf-8"))
        events = json.loads((CACHE / "events" / f"{code}.json").read_text(encoding="utf-8"))
        text = render_one(
            quote,
            profiles.get(code),
            finance.get(code, []),
            mainop.get(code, []),
            holders.get(code, []),
            relations.get(code, []),
            holdings.get(code, []),
            review,
            events,
        )
        folder = STOCKS / code
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "stock.md").write_text(text, encoding="utf-8")
        manifest.append(f"{code}\t{clean_text(quote.get('f14'))}\t{code}.{prefix}\t{market}")
    (STOCKS / "manifest.tsv").write_text("\n".join(manifest) + "\n", encoding="utf-8")
    print(f"[render] files={len(quotes)}", flush=True)


def report_count(report: str, filt: str, sort: str) -> int:
    last_message = ""
    for attempt in range(8):
        payload = fetch_json(datacenter_url(report, "SECUCODE", 1, 1, filt, sort, True), timeout=30, retries=3)
        last_message = str(payload.get("message") or "")
        if any(word in last_message for word in ("繁忙", "超时", "稍后", "频繁", "系统异常")):
            time.sleep(min(20, 1.2 * (attempt + 1)))
            continue
        result = payload.get("result") or {}
        return int(result.get("count") or 0)
    raise RuntimeError(f"count failed for {report}: {last_message}")


def load_code_slices(
    cache_name: str,
    report: str,
    columns: str,
    base_filt: str,
    sort: str,
    start: str,
    end: str,
) -> list[dict]:
    """Page through a code range. Splits ranges the endpoint refuses past page 100."""
    filt = f'{base_filt}(SECURITY_CODE>="{start}")(SECURITY_CODE<="{end}")'
    count = report_count(report, filt, sort)
    if count == 0:
        return []
    label = f"{cache_name}_{start}_{end}"
    if count > 49000 and int(start) < int(end):
        mid = (int(start) + int(end)) // 2
        print(f"[split] {label} count={count}", flush=True)
        left = load_code_slices(cache_name, report, columns, base_filt, sort, start, f"{mid:06d}")
        right = load_code_slices(cache_name, report, columns, base_filt, sort, f"{mid + 1:06d}", end)
        return left + right
    if count > 49000:
        raise RuntimeError(f"{label} still has {count} rows after splitting")
    rows = load_pages(label, report, columns, filt, sort, True)
    if len(rows) < count * 0.98:
        raise RuntimeError(f"{label} incomplete: got {len(rows)} of {count}")
    return rows


def download_dated(
    cache_prefix: str,
    report: str,
    columns: str,
    date_field: str,
    dates: list[str],
    sort: str,
    split_codes: bool = False,
) -> list[dict]:
    merged_path = CACHE / f"{cache_prefix}_merged.json"
    if merged_path.exists():
        rows = json.loads(merged_path.read_text(encoding="utf-8"))
        print(f"[cache] {cache_prefix} merged={len(rows)}", flush=True)
        return rows
    rows: list[dict] = []
    for day in dates:
        base = f"({date_field}='{day}')"
        if split_codes:
            total = report_count(report, base, sort)
            part = load_code_slices(f"{cache_prefix}_{day}", report, columns, base, sort, "000000", "999999")
            if total and len(part) < total * 0.98:
                raise RuntimeError(f"{cache_prefix} {day} got {len(part)} of {total}")
        else:
            part = load_pages(
                f"{cache_prefix}_{day}",
                report,
                columns,
                base,
                sort,
                True,
            )
        rows.extend(part)
        print(f"[fetch] {cache_prefix} {day} rows={len(part)}", flush=True)
    merged_path.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    return rows


def download_split(cache_prefix: str, report: str, columns: str, sort: str) -> list[dict]:
    merged_path = CACHE / f"{cache_prefix}_merged.json"
    if merged_path.exists():
        rows = json.loads(merged_path.read_text(encoding="utf-8"))
        print(f"[cache] {cache_prefix} merged={len(rows)}", flush=True)
        return rows
    ranged = '(SECURITY_CODE>="000000")(SECURITY_CODE<="999999")'
    total = report_count(report, ranged, sort)
    rows = load_code_slices(cache_prefix, report, columns, "", sort, "000000", "999999")
    if total and len(rows) < total * 0.98:
        raise RuntimeError(f"{cache_prefix} got {len(rows)} of {total}")
    merged_path.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    print(f"[fetch] {cache_prefix} rows={len(rows)}", flush=True)
    return rows


def download_all() -> list[dict]:
    CACHE.mkdir(parents=True, exist_ok=True)
    quotes = fetch_quotes()
    uniq = {}
    for row in quotes:
        uniq[str(row["f12"])] = row
    quotes = [row for row in uniq.values() if is_listed_quote(row)]
    print(f"[filter] listed={len(quotes)}", flush=True)
    load_pages(
        "basic",
        "RPT_F10_BASIC_ORGINFO",
        "ALL",
        '(SECURITY_TYPEE="A股")',
        "SECURITY_CODE",
        False,
    )
    finance_columns = ",".join(
        [
            "SECUCODE",
            "SECURITY_CODE",
            "SECURITY_NAME_ABBR",
            "ORG_TYPE",
            "REPORT_DATE",
            "REPORT_TYPE",
            "REPORT_DATE_NAME",
            "NOTICE_DATE",
            "CURRENCY",
            *[field for field, _, _ in FINANCE_FIELDS],
        ]
    )
    download_dated(
        "finance",
        "RPT_F10_FINANCE_MAINFINADATA",
        finance_columns,
        "REPORT_DATE",
        REPORT_DATES,
        "REPORT_DATE",
    )
    download_dated(
        "mainop",
        "RPT_F10_FN_MAINOP",
        "SECUCODE,SECURITY_CODE,REPORT_DATE,MAINOP_TYPE,ITEM_NAME,MAIN_BUSINESS_INCOME,MBI_RATIO,GROSS_RPOFIT_RATIO",
        "REPORT_DATE",
        MAINOP_DATES,
        "REPORT_DATE",
    )
    download_dated(
        "holders",
        "RPT_F10_EH_HOLDERS",
        "SECUCODE,SECURITY_CODE,END_DATE,HOLDER_RANK,HOLDER_NAME,HOLD_NUM,HOLD_NUM_RATIO,HOLD_NUM_CHANGE,HOLDER_STATE",
        "END_DATE",
        HOLDER_DATES,
        "END_DATE",
        split_codes=True,
    )
    load_pages(
        "relation",
        "RPT_F10_EH_RELATION",
        "SECUCODE,SECURITY_CODE,HOLDER_NAME,RELATED_RELATION,HOLD_RATIO,END_DATE",
        "",
        "SECUCODE",
        False,
    )
    download_split(
        "holdings",
        "RPT_F10_PUBLIC_OP_HOLDINGORG",
        "SECUCODE,SECURITY_CODE,HOLD_ORG_NAME,REG_CAPITAL,ORG_HOLD_RATIO,HOLD_TYPE,ACTUAL_INVEST_AMT,NETPROFIT,MAIN_PRODUCTS,REPORT_DATE",
        "SECUCODE",
    )
    return quotes


def fetch_all_pages(report: str, columns: str, filt: str, sort: str) -> list[dict]:
    rows: list[dict] = []
    page = 1
    count = None
    while True:
        payload = fetch_json(datacenter_url(report, columns, page, 500, filt, sort, True))
        result = payload.get("result") or {}
        if count is None:
            count = int(result.get("count") or 0)
        data = result.get("data") or []
        if not data:
            break
        rows.extend(data)
        if len(rows) >= count:
            break
        page += 1
        if page > 20:
            raise RuntimeError(f"too many pages for {report} {filt}")
    return rows


def build_subset(codes: list[str], workers: int) -> None:
    """Render a few codes with per-code queries, without the full-market download."""
    quotes = fetch_quotes()
    wanted = {code: row for code, row in ((str(row["f12"]), row) for row in quotes) if str(row["f12"]) in set(codes)}
    missing = [code for code in codes if code not in wanted]
    if missing:
        raise RuntimeError(f"codes not in listed universe: {missing}")
    selected = [wanted[code] for code in codes]
    ensure_per_stock(selected, workers)
    finance_columns = ",".join(
        [
            "SECUCODE",
            "SECURITY_CODE",
            "SECURITY_NAME_ABBR",
            "ORG_TYPE",
            "REPORT_DATE",
            "REPORT_TYPE",
            "REPORT_DATE_NAME",
            "NOTICE_DATE",
            "CURRENCY",
            *[field for field, _, _ in FINANCE_FIELDS],
        ]
    )
    STOCKS.mkdir(parents=True, exist_ok=True)
    for quote in selected:
        code = str(quote["f12"])
        print(f"[subset] {code}", flush=True)
        profile_rows = fetch_all_pages(
            "RPT_F10_BASIC_ORGINFO",
            "ALL",
            f'(SECURITY_CODE="{code}")',
            "SECURITY_CODE",
        )
        profile = profile_rows[0] if profile_rows else None
        finance = fetch_all_pages(
            "RPT_F10_FINANCE_MAINFINADATA",
            finance_columns,
            f'(SECURITY_CODE="{code}")',
            "REPORT_DATE",
        )
        finance = [row for row in finance if date_only(row.get("REPORT_DATE")) in REPORT_DATES]
        mainop = fetch_all_pages(
            "RPT_F10_FN_MAINOP",
            "SECUCODE,SECURITY_CODE,REPORT_DATE,MAINOP_TYPE,ITEM_NAME,MAIN_BUSINESS_INCOME,MBI_RATIO,GROSS_RPOFIT_RATIO",
            f'(SECURITY_CODE="{code}")',
            "REPORT_DATE",
        )
        mainop = [row for row in mainop if date_only(row.get("REPORT_DATE")) in MAINOP_DATES]
        holders = fetch_all_pages(
            "RPT_F10_EH_HOLDERS",
            "SECUCODE,SECURITY_CODE,END_DATE,HOLDER_RANK,HOLDER_NAME,HOLD_NUM,HOLD_NUM_RATIO,HOLD_NUM_CHANGE,HOLDER_STATE",
            f'(SECURITY_CODE="{code}")',
            "END_DATE",
        )
        holders = [row for row in holders if date_only(row.get("END_DATE")) in HOLDER_DATES]
        relations = fetch_all_pages(
            "RPT_F10_EH_RELATION",
            "SECUCODE,SECURITY_CODE,HOLDER_NAME,RELATED_RELATION,HOLD_RATIO,END_DATE",
            f'(SECURITY_CODE="{code}")',
            "END_DATE",
        )
        holdings = fetch_all_pages(
            "RPT_F10_PUBLIC_OP_HOLDINGORG",
            "SECUCODE,SECURITY_CODE,HOLD_ORG_NAME,REG_CAPITAL,ORG_HOLD_RATIO,HOLD_TYPE,ACTUAL_INVEST_AMT,NETPROFIT,MAIN_PRODUCTS,REPORT_DATE",
            f'(SECURITY_CODE="{code}")',
            "REPORT_DATE",
        )
        review = json.loads((CACHE / "review" / f"{code}.json").read_text(encoding="utf-8"))
        events = json.loads((CACHE / "events" / f"{code}.json").read_text(encoding="utf-8"))
        text = render_one(quote, profile, finance, mainop, holders, relations, holdings, review, events)
        folder = STOCKS / code
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "stock.md").write_text(text, encoding="utf-8")
        print(f"[subset] wrote {folder / 'stock.md'} chars={len(text)}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--only-render", action="store_true")
    parser.add_argument("--codes", default="", help="comma-separated codes; skips the full-market download")
    args = parser.parse_args()
    started = time.time()
    try:
        if args.codes:
            codes = [item.strip() for item in args.codes.split(",") if item.strip()]
            build_subset(codes, args.workers)
        else:
            quotes = download_all()
            if not args.only_render:
                ensure_per_stock(quotes, args.workers)
            render_all(quotes)
    except Exception:
        traceback.print_exc()
        raise
    print(f"[done] seconds={time.time() - started:.1f}", flush=True)


if __name__ == "__main__":
    main()
