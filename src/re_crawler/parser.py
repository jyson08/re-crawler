from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

LOGGER = logging.getLogger(__name__)

OUTPUT_COLUMNS = [
    "시",
    "구",
    "동",
    "단지",
    "준공연도(연식)",
    "나이",
    "전체세대수",
    "공급면적",
    "타입세대수",
    "평형",
    "전용면적",
    "방갯수",
    "화장실갯수",
    "매매가격",
    "전세가격",
    "전세가율",
]


@dataclass
class RawAreaPrice:
    supply_area_m2: float | None
    exclusive_area_m2: float | None
    type_households: int | None
    room_count: int | None
    bath_count: int | None
    sale_price_text: str | None
    lease_price_text: str | None


@dataclass
class RawComplexData:
    city: str | None
    district: str | None
    dong: str | None
    complex_name: str | None
    completion_year: int | None
    total_households: int | None
    area_prices: list[RawAreaPrice]


def _parse_single_korean_price(text: str) -> int | None:
    cleaned = re.sub(r"[,\s]", "", text)
    if not cleaned:
        return None

    match = re.fullmatch(r"(?:(\d+)억)?(?:(\d+))?", cleaned)
    if match:
        eok = int(match.group(1)) if match.group(1) else 0
        man = int(match.group(2)) if match.group(2) else 0
        return eok * 10000 + man

    num_match = re.search(r"(\d+)", cleaned)
    if num_match:
        return int(num_match.group(1))
    return None


def parse_korean_price_to_manwon(text: str | None) -> int | None:
    if not text:
        return None

    parts = re.split(r"[~-]", text)
    values = [_parse_single_korean_price(part) for part in parts if part.strip()]
    values = [v for v in values if v is not None]

    if not values:
        return None
    if len(values) == 1:
        return values[0]
    return int(round(sum(values) / len(values)))


def calc_pyung(supply_area_m2: float | None) -> float | None:
    if supply_area_m2 is None:
        return None
    return round(supply_area_m2 * 0.3025, 1)


def calc_age(completion_year: int | None, today: date | None = None) -> int | None:
    if completion_year is None:
        return None
    current_year = (today or date.today()).year
    if completion_year > current_year:
        LOGGER.warning("Invalid completion year: %s", completion_year)
        return None
    return current_year - completion_year


def calc_lease_ratio(lease_price: int | None, sale_price: int | None) -> float | None:
    if lease_price is None or sale_price in (None, 0):
        return None
    return round((lease_price / sale_price) * 100, 1)


def _round1(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 1)


def build_dataframe(raw: RawComplexData) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    age = calc_age(raw.completion_year)

    for ap in raw.area_prices:
        sale_price = parse_korean_price_to_manwon(ap.sale_price_text)
        lease_price = parse_korean_price_to_manwon(ap.lease_price_text)

        row = {
            "시": raw.city,
            "구": raw.district,
            "동": raw.dong,
            "단지": raw.complex_name,
            "준공연도(연식)": raw.completion_year,
            "나이": age,
            "전체세대수": raw.total_households,
            "공급면적": _round1(ap.supply_area_m2),
            "타입세대수": ap.type_households,
            "평형": calc_pyung(ap.supply_area_m2),
            "전용면적": _round1(ap.exclusive_area_m2),
            "방갯수": ap.room_count,
            "화장실갯수": ap.bath_count,
            "매매가격": sale_price,
            "전세가격": lease_price,
            "전세가율": calc_lease_ratio(lease_price, sale_price),
        }
        rows.append(row)

    if not rows:
        rows.append({col: None for col in OUTPUT_COLUMNS})
        rows[0]["시"] = raw.city
        rows[0]["구"] = raw.district
        rows[0]["동"] = raw.dong
        rows[0]["단지"] = raw.complex_name
        rows[0]["준공연도(연식)"] = raw.completion_year
        rows[0]["나이"] = age
        rows[0]["전체세대수"] = raw.total_households

    return pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
