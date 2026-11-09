#!/usr/bin/env python3
"""Check that generated stock profiles match the listed A-share universe."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from build_a_share_docs import is_listed_quote  # noqa: E402

STOCKS = ROOT / "stocks"
CACHE = ROOT / "data" / "cache"

REQUIRED = [
    "## 股票介绍",
    "## 公司介绍",
    "## 财报介绍",
    "## 业务介绍",
    "## 上下游链路介绍",
    "## 投资、控股介绍",
    "## 风险",
]


def main() -> None:
    meta = json.loads((CACHE / "quotes" / "_meta.json").read_text(encoding="utf-8"))
    listed = []
    for page in range(1, meta["pages"] + 1):
        rows = json.loads((CACHE / "quotes" / f"{page:04d}.json").read_text(encoding="utf-8"))
        listed.extend(str(row["f12"]) for row in rows if is_listed_quote(row))
    listed = sorted(set(listed))
    folders = sorted(path.name for path in STOCKS.iterdir() if path.is_dir())
    failures = []
    missing = [code for code in listed if not (STOCKS / code / "stock.md").exists()]
    extra = [name for name in folders if name not in set(listed)]
    if missing:
        failures.append(f"missing stock.md: {len(missing)} e.g. {missing[:5]}")
    if extra:
        failures.append(f"unexpected folders: {extra[:10]}")
    if (STOCKS / "000003").exists() or (STOCKS / "002731").exists():
        failures.append("delisted sample was included")

    checked = 0
    request_failures = 0
    for code in listed:
        path = STOCKS / code / "stock.md"
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        checked += 1
        if not text.startswith("---\nname: ") or f'name: "{code}"' not in text.split("---", 2)[1]:
            failures.append(f"{code} frontmatter mismatch")
            break
        for heading in REQUIRED:
            if heading not in text:
                failures.append(f"{code} missing {heading}")
                break
        if "请求失败" in text:
            request_failures += 1
    print(f"listed={len(listed)} checked={checked} request_failures={request_failures}")

    samples = {
        "600519": ["贵州茅台酒股份有限公司", "中国贵州茅台酒厂(集团)有限责任公司", "54.50%"],
        "000001": ["平安银行股份有限公司", "存款总额", "000001.SZ"],
        "300750": ["动力电池系统", "曾毓群", "零碳新能源科技公司"],
        "920992": ["中科美菱低温科技股份有限公司", "920992.BJ"],
        "000831": ["中国稀土"],
    }
    for code, needles in samples.items():
        text = (STOCKS / code / "stock.md").read_text(encoding="utf-8")
        for needle in needles:
            if needle not in text:
                failures.append(f"{code} missing fact {needle}")
        if code == "000831" and "最新价：-" in text:
            failures.append("000831 still shows a dash price")

    if request_failures > max(20, int(checked * 0.01)):
        failures.append(f"too many request failures: {request_failures}")
    if failures:
        print("\n".join(failures))
        sys.exit(1)
    print("ok")


if __name__ == "__main__":
    main()
