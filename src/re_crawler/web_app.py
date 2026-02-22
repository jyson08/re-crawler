from __future__ import annotations

import math
import sys
import inspect
import os
import json
from pathlib import Path

import pydeck as pdk
import streamlit as st
import streamlit.components.v1 as components

# Streamlit Cloud runs this file directly, so ensure `src` is on sys.path.
SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import re_crawler.auto_excel as ae

ADSENSE_CLIENT = "ca-pub-3193725081286573"
ADSENSE_SLOT_TOP = os.getenv("ADSENSE_SLOT_TOP", "").strip()
ADSENSE_SLOT_MID = os.getenv("ADSENSE_SLOT_MID", "").strip()
ADSENSE_SLOT_BOTTOM = os.getenv("ADSENSE_SLOT_BOTTOM", "").strip()


def _inject_adsense_script() -> None:
    components.html(
        f"""
        <script async src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client={ADSENSE_CLIENT}" crossorigin="anonymous"></script>
        """,
        height=0,
    )


def _render_adsense_slot(slot_id: str, height: int = 120) -> None:
    if not slot_id:
        return
    components.html(
        f"""
        <ins class="adsbygoogle"
             style="display:block"
             data-ad-client="{ADSENSE_CLIENT}"
             data-ad-slot="{slot_id}"
             data-ad-format="auto"
             data-full-width-responsive="true"></ins>
        <script>
          (adsbygoogle = window.adsbygoogle || []).push({{}});
        </script>
        """,
        height=height,
    )


def _render_info_pages(page: str) -> None:
    if page == "About":
        st.subheader("서비스 소개")
        st.markdown(
            """
            이 서비스는 입력한 단지를 기준으로 주변 단지 데이터를 수집해 비교표와 엑셀을 생성합니다.
            주요 기능은 후보 단지 선택 수집, 지도 확인, 엑셀 다운로드입니다.
            """
        )
    elif page == "이용안내":
        st.subheader("이용안내")
        st.markdown(
            """
            1. 동(선택)과 단지명을 입력하고 `후보 조회`를 누릅니다.  
            2. 후보 목록에서 수집할 단지를 체크합니다.  
            3. `수집하기`를 눌러 결과표를 확인하고 엑셀을 다운로드합니다.
            """
        )
    elif page == "개인정보처리방침":
        st.subheader("개인정보처리방침")
        st.markdown(
            """
            - 본 서비스는 사용자가 입력한 검색어를 수집 처리 목적으로만 사용합니다.  
            - 입력값과 결과 파일은 운영 및 오류 분석 목적 범위에서만 보관될 수 있습니다.  
            - 개인정보 관련 문의는 아래 문의 채널로 요청해 주세요.
            """
        )
    elif page == "문의":
        st.subheader("문의")
        st.markdown(
            """
            - 이메일: `your-email@example.com`  
            - 이슈 제보: GitHub Issues  
            - 응답 시간: 영업일 기준 1~3일
            """
        )


def _save_stem_from_query(raw_query: str) -> str:
    queries = ae.split_queries(raw_query)
    if not queries:
        return "complex"
    return queries[0] if len(queries) == 1 else f"{queries[0]}_외{len(queries)-1}"


def _call_collect_dataset_compat(**kwargs):
    fn = ae.collect_dataset
    sig = inspect.signature(fn)
    allowed = {k: v for k, v in kwargs.items() if k in sig.parameters}
    out = fn(**allowed)
    if isinstance(out, tuple):
        if len(out) == 5:
            return out
        if len(out) == 4:
            result_df, selected_info, crawled_info, markers_df = out
            return result_df, selected_info, crawled_info, markers_df, []
    raise RuntimeError("collect_dataset returned unexpected shape")


@st.cache_data(ttl=3600, show_spinner=False)
def _cached_kb_index() -> list[dict]:
    session = ae.create_kb_session()
    items = ae.fetch_kb_complex_index(session)
    # Do not keep empty snapshots in cache; they are usually transient failures.
    if not items:
        raise RuntimeError("empty_kb_index")
    return items


def _kb_index_cache_file() -> Path:
    p = Path("./output/kb_index_cache.json")
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _load_kb_index_file_cache() -> list[dict]:
    p = _kb_index_cache_file()
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    return [x for x in data if isinstance(x, dict)]


def _save_kb_index_file_cache(items: list[dict]) -> None:
    if not items:
        return
    p = _kb_index_cache_file()
    try:
        p.write_text(json.dumps(items, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def _get_kb_index_resilient() -> list[dict]:
    # 1) Try streamlit cache.
    try:
        items = _cached_kb_index()
        if items:
            _save_kb_index_file_cache(items)
            return items
    except Exception:
        pass

    # 2) Clear stale empty cache and retry direct once.
    try:
        _cached_kb_index.clear()
    except Exception:
        pass
    try:
        session = ae.create_kb_session()
        items = ae.fetch_kb_complex_index(session)
        if items:
            _save_kb_index_file_cache(items)
            return items
    except Exception:
        pass

    # 3) Fallback to last successful local snapshot.
    return _load_kb_index_file_cache()


def _build_label_text(row) -> str:
    # Keep map labels short so they remain visible at most zoom levels.
    return str(row.get("complex_name") or "")


def _to_float_safe(v) -> float | None:
    try:
        if v is None:
            return None
        n = float(str(v).replace("%", "").replace(",", "").strip())
        if not math.isfinite(n):
            return None
        return n
    except Exception:
        return None


def _to_bool_safe(v) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in {"1", "true", "t", "y", "yes"}


def _to_int_safe(v) -> int | None:
    try:
        if v is None:
            return None
        return int(float(str(v).replace(",", "").strip()))
    except Exception:
        return None


def _fmt_money(v) -> str | None:
    n = _to_int_safe(v)
    if n is None:
        return None
    return f"{n:,}"


def _fmt_households(v) -> str | None:
    n = _to_int_safe(v)
    if n is None:
        return None
    return f"{n:,}"


def _fmt_area(v) -> str | None:
    n = _to_float_safe(v)
    if n is None:
        return None
    text = f"{n:.1f}"
    if 0 <= n < 10 and not text.startswith("0"):
        text = f"0{text}"
    return text


def _fmt_ratio(v) -> str | None:
    n = _to_float_safe(v)
    if n is None:
        return None
    if abs(n) < 1e-9:
        return "0%"
    if abs(n - round(n)) < 1e-9:
        return f"{int(round(n)):+d}%"
    return f"{n:+.1f}%"


def _fmt_far(v) -> str | None:
    n = _to_float_safe(v)
    if n is None:
        return None
    return f"{int(round(n))}%"


def _fmt_distance_m(v) -> str | None:
    n = _to_float_safe(v)
    if n is None:
        return None
    return f"{int(round(n))}"


def _build_web_display_df(df):
    out = df.copy()

    money_cols = [
        ae.COL_RECENT_SALE,
        ae.COL_SALE,
        ae.COL_MIN_ASK_SALE,
        ae.COL_RECENT_LEASE,
        ae.COL_LEASE,
        ae.COL_MIN_ASK_LEASE,
    ]
    for c in money_cols:
        if c in out.columns:
            out[c] = out[c].map(_fmt_money)

    if ae.COL_TOTAL_HOUSEHOLDS in out.columns:
        out[ae.COL_TOTAL_HOUSEHOLDS] = out[ae.COL_TOTAL_HOUSEHOLDS].map(_fmt_households)

    for c in [ae.COL_SUPPLY, ae.COL_PYUNG, ae.COL_EXCLUSIVE]:
        if c in out.columns:
            out[c] = out[c].map(_fmt_area)

    for c in [ae.COL_UNDERVALUE_RATIO, ae.COL_GAP_RATIO_LEASE, ae.COL_LEASE_RATIO]:
        if c in out.columns:
            out[c] = out[c].map(_fmt_ratio)

    if ae.COL_FAR in out.columns:
        out[ae.COL_FAR] = out[ae.COL_FAR].map(_fmt_far)
    if ae.COL_ELEM_DISTANCE in out.columns:
        out[ae.COL_ELEM_DISTANCE] = out[ae.COL_ELEM_DISTANCE].map(_fmt_distance_m)

    return out


def _styled_result_df(df):
    raw_df = df
    display_df = _build_web_display_df(df)
    sale_gap_col = ae.COL_UNDERVALUE_RATIO
    lease_gap_col = ae.COL_GAP_RATIO_LEASE
    lease_ratio_col = ae.COL_LEASE_RATIO
    if sale_gap_col not in raw_df.columns and lease_gap_col not in raw_df.columns and lease_ratio_col not in raw_df.columns:
        return display_df

    def _row_style(row):
        sale_gap = _to_float_safe(raw_df.at[row.name, sale_gap_col]) if (row.name in raw_df.index and sale_gap_col in raw_df.columns) else None
        lease_gap = _to_float_safe(raw_df.at[row.name, lease_gap_col]) if (row.name in raw_df.index and lease_gap_col in raw_df.columns) else None
        lease_ratio = _to_float_safe(raw_df.at[row.name, lease_ratio_col]) if (row.name in raw_df.index and lease_ratio_col in raw_df.columns) else None
        if (
            (sale_gap is not None and sale_gap < 0.0)
            or (lease_gap is not None and lease_gap < 0.0)
            or (lease_ratio is not None and lease_ratio >= 65.0)
        ):
            return ["background-color: #DCE6F1"] * len(display_df.columns)
        return [""] * len(display_df.columns)

    def _cause_style(row):
        styles = [""] * len(display_df.columns)
        idx_map = {c: i for i, c in enumerate(display_df.columns)}
        sale_gap = _to_float_safe(raw_df.at[row.name, sale_gap_col]) if (row.name in raw_df.index and sale_gap_col in raw_df.columns) else None
        lease_gap = _to_float_safe(raw_df.at[row.name, lease_gap_col]) if (row.name in raw_df.index and lease_gap_col in raw_df.columns) else None
        lease_ratio = _to_float_safe(raw_df.at[row.name, lease_ratio_col]) if (row.name in raw_df.index and lease_ratio_col in raw_df.columns) else None
        if sale_gap is not None and sale_gap < 0.0 and sale_gap_col in idx_map:
            styles[idx_map[sale_gap_col]] = "color: #1F4E78; font-weight: 700"
        if lease_gap is not None and lease_gap < 0.0 and lease_gap_col in idx_map:
            styles[idx_map[lease_gap_col]] = "color: #1F4E78; font-weight: 700"
        if lease_ratio is not None and lease_ratio >= 65.0 and lease_ratio_col in idx_map:
            styles[idx_map[lease_ratio_col]] = "color: #1F4E78; font-weight: 700"
        return styles

    return display_df.style.apply(_row_style, axis=1).apply(_cause_style, axis=1)


def _enrich_marker_names(markers_df, preview_df):
    if markers_df is None or getattr(markers_df, "empty", True):
        return markers_df
    if preview_df is None or getattr(preview_df, "empty", True):
        return markers_df
    if "complex_id" not in markers_df.columns or "complex_id" not in preview_df.columns:
        return markers_df

    # Find the most likely "complex name" column from preview table.
    name_col = None
    for c in ["단지명", "단지", "complex_name"]:
        if c in preview_df.columns:
            name_col = c
            break
    if name_col is None:
        for c in preview_df.columns:
            if c not in {"complex_id", "seed_query", "is_seed", "수집"}:
                name_col = c
                break
    if name_col is None:
        return markers_df

    name_map = (
        preview_df[["complex_id", name_col]]
        .dropna(subset=["complex_id"])
        .drop_duplicates(subset=["complex_id"], keep="first")
        .set_index("complex_id")[name_col]
        .to_dict()
    )
    out = markers_df.copy()
    out["complex_name"] = out.apply(
        lambda r: name_map.get(r.get("complex_id")) or r.get("complex_name"),
        axis=1,
    )
    return out


def _circle_polygon(lat: float, lng: float, radius_m: float, points: int = 72) -> list[list[float]]:
    # Approximate geodesic circle around (lat, lng) in WGS84.
    earth_r = 6378137.0
    lat_rad = math.radians(lat)
    out: list[list[float]] = []
    for i in range(points):
        theta = 2.0 * math.pi * i / points
        dlat = (radius_m / earth_r) * math.sin(theta)
        dlng = (radius_m / (earth_r * max(math.cos(lat_rad), 1e-8))) * math.cos(theta)
        out.append([lng + math.degrees(dlng), lat + math.degrees(dlat)])
    return out


def _circle_dashed_paths(
    lat: float,
    lng: float,
    radius_m: float,
    points: int = 180,
    dash_step: int = 4,
    gap_step: int = 3,
) -> list[list[list[float]]]:
    ring = _circle_polygon(lat, lng, radius_m, points=points)
    paths: list[list[list[float]]] = []
    step = max(1, dash_step + gap_step)
    for i in range(0, points, step):
        seg: list[list[float]] = []
        for j in range(dash_step + 1):
            idx = (i + j) % points
            seg.append(ring[idx])
        if len(seg) >= 2:
            paths.append(seg)
    return paths


def _render_map(markers_df, radius_m: float):
    if markers_df.empty:
        st.info("\uc9c0\ub3c4\uc5d0 \ud45c\uc2dc\ud560 \uc88c\ud45c \ub370\uc774\ud130\uac00 \uc5c6\uc2b5\ub2c8\ub2e4.")
        return

    map_df = markers_df.copy()
    map_df["lat"] = map_df["lat"].map(_to_float_safe)
    map_df["lng"] = map_df["lng"].map(_to_float_safe)
    map_df = map_df.dropna(subset=["lat", "lng"]).copy()
    if map_df.empty:
        st.info("\uc9c0\ub3c4\uc5d0 \ud45c\uc2dc\ud560 \uc88c\ud45c \ub370\uc774\ud130\uac00 \uc5c6\uc2b5\ub2c8\ub2e4.")
        return

    map_df["is_seed"] = map_df["is_seed"].map(_to_bool_safe)
    if not bool(map_df["is_seed"].any()):
        map_df.loc[map_df.index[0], "is_seed"] = True

    view = pdk.ViewState(
        latitude=float(map_df["lat"].mean()),
        longitude=float(map_df["lng"].mean()),
        zoom=13,
        pitch=0,
    )

    map_df["color"] = map_df["is_seed"].map(lambda x: [220, 53, 69, 180] if x else [52, 152, 219, 170])
    map_df["label_text"] = map_df.apply(_build_label_text, axis=1)
    # TextLayer needs an explicit character set for non-Latin labels.
    label_charset = "".join(sorted(set("".join(map_df["label_text"].dropna().astype(str).tolist()))))

    seed_df = map_df[map_df["is_seed"] == True].copy()
    radius_dash_rows = []
    safe_radius = max(1.0, float(radius_m))
    if not seed_df.empty:
        seed_df["radius_paths"] = seed_df.apply(
            lambda r: _circle_dashed_paths(float(r["lat"]), float(r["lng"]), safe_radius),
            axis=1,
        )
        seed_df["radius_label"] = f"\ubc18\uacbd: {int(round(safe_radius))}m"
        for _, row in seed_df.iterrows():
            for path in row["radius_paths"]:
                radius_dash_rows.append({"path": path})

    marker_layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_df,
        get_position="[lng, lat]",
        get_radius=22,
        get_fill_color="color",
        pickable=True,
    )
    radius_layer = pdk.Layer(
        "PathLayer",
        data=radius_dash_rows,
        get_path="path",
        get_color=[70, 70, 70, 220],
        get_width=2,
        width_min_pixels=2,
        rounded=True,
        pickable=False,
    )
    text_layer = pdk.Layer(
        "TextLayer",
        data=map_df,
        get_position="[lng, lat]",
        get_text="label_text",
        get_color=[20, 20, 20, 255],
        get_size=15,
        size_units="pixels",
        size_min_pixels=12,
        size_max_pixels=24,
        character_set=label_charset,
        font_family="Malgun Gothic, Apple SD Gothic Neo, NanumGothic, sans-serif",
        get_text_anchor="'start'",
        get_alignment_baseline="'top'",
        get_pixel_offset=[10, 10],
        billboard=True,
        pickable=False,
    )
    radius_text_layer = pdk.Layer(
        "TextLayer",
        data=seed_df,
        get_position="[lng, lat]",
        get_text="radius_label",
        get_color=[220, 53, 69, 230],
        get_size=16,
        size_units="pixels",
        get_text_anchor="middle",
        get_alignment_baseline="center",
        get_pixel_offset=[0, -18],
        billboard=True,
        pickable=False,
    )

    tooltip = {
        "html": (
            "<b>{complex_name}</b><br/>"
            "\uc900\uacf5\ub144: {built_year}<br/>"
            "\uc138\ub300\uc218: {households}<br/>"
            "\uc8fc\ucc28\ub300\uc218: {parking}<br/>"
            "\ud604\uad00\uad6c\uc870: {hall_type}"
        ),
        "style": {"backgroundColor": "white", "color": "black"},
    }

    st.pydeck_chart(
        pdk.Deck(
            map_provider="carto",
            map_style="light",
            initial_view_state=view,
            layers=[marker_layer, radius_layer, text_layer, radius_text_layer],
            tooltip=tooltip,
        )
    )


def main() -> None:
    st.set_page_config(page_title="KB 부동산 단지 크롤링", layout="wide")
    _inject_adsense_script()
    st.title("KB 부동산 단지 크롤링")

    with st.sidebar:
        page = st.radio("메뉴", ["크롤링", "About", "이용안내", "개인정보처리방침", "문의"], index=0)
        if page == "크롤링":
            st.subheader("수집 옵션")
            dong = st.text_input("동(읍/면/동)", value="")
            st.caption("동명이인 단지가 많으면 동명을 먼저 입력하세요. 예: 응암동")
            query = st.text_input("단지명", value="백련산SK뷰아이파크")
            radius_m = st.number_input("반경(m)", min_value=100, max_value=5000, value=500, step=100)
            min_households = st.number_input("최소 세대수", min_value=1, max_value=10000, value=290, step=10)
            preview_clicked = st.button("후보 조회", type="primary")
        else:
            dong = ""
            query = ""
            radius_m = 500
            min_households = 290
            preview_clicked = False

    if page != "크롤링":
        _render_adsense_slot(ADSENSE_SLOT_TOP, height=120)
        _render_info_pages(page)
        _render_adsense_slot(ADSENSE_SLOT_BOTTOM, height=120)
        return

    _render_adsense_slot(ADSENSE_SLOT_TOP, height=120)
    if "has_result" not in st.session_state:
        st.session_state["has_result"] = False
    has_preview_api = hasattr(ae, "preview_candidates")

    def _on_progress(event: dict) -> None:
        if event.get("event") == "prepare":
            stage = event.get("stage")
            msg = event.get("message") or "처리 준비 중..."
            if stage == "index_start":
                progress_bar.progress(5, text=msg)
            elif stage == "index_done":
                progress_bar.progress(12, text=msg)
            elif stage == "preview_seed":
                progress_bar.progress(20, text=msg)
            else:
                progress_bar.progress(15, text=msg)
            progress_text.caption(msg)
        elif event.get("event") == "query_target_ready":
            total = int(event.get("total") or 0)
            q = event.get("query")
            progress_text.info(f"[{q}] 대상 후보 {total}개 확인")
            if total <= 0:
                progress_bar.progress(100, text=f"[{q}] 처리할 후보가 없습니다.")
            else:
                progress_bar.progress(0, text=f"[{q}] 0/{total} 처리 중")
        elif event.get("event") == "query_progress":
            total = max(1, int(event.get("total") or 1))
            current = min(total, int(event.get("current") or 0))
            q = event.get("query")
            name = event.get("complex_name") or ""
            pct = int((current / total) * 100)
            progress_bar.progress(pct, text=f"[{q}] {current}/{total} 처리 중")
            if name:
                progress_text.caption(f"현재 단지: {name}")

    if preview_clicked:
        if not query.strip():
            st.error("단지명을 입력해 주세요.")
            return

        progress_bar = st.progress(0, text="수집 준비 중...")
        progress_text = st.empty()
        preview_started_at = __import__("time").perf_counter()
        with st.spinner("데이터 수집 중입니다..."):
            try:
                progress_bar.progress(3, text="단지 인덱스 캐시 확인 중...")
                index_items = _get_kb_index_resilient()
                if has_preview_api:
                    preview_df, preview_markers_df, candidate_ids, _selected = ae.preview_candidates(
                        raw_query=query.strip(),
                        radius_m=float(radius_m),
                        min_households=int(min_households),
                        fast_mode=True,
                        max_dong_codes=None,
                        index_items=index_items,
                        preferred_dong=(dong.strip() if dong.strip() else None),
                        progress_callback=_on_progress,
                        max_preview_candidates=70,
                    )
                else:
                    # Backward-compatible fallback when older module is loaded on cloud.
                    result_df, _selected_info, crawled_info, markers_df, _metrics = _call_collect_dataset_compat(
                        raw_query=query.strip(),
                        radius_m=float(radius_m),
                        min_households=int(min_households),
                        fast_mode=True,
                        max_dong_codes=None,
                        index_items=index_items,
                        preferred_dong=(dong.strip() if dong.strip() else None),
                        progress_callback=_on_progress,
                    )
                    preview_df = (
                        result_df[["단지"]]
                        .drop_duplicates()
                        .rename(columns={"단지": "단지명"})
                        .reset_index(drop=True)
                    )
                    name_to_id = {
                        str(r.get("complex_name")): int(r.get("complex_id"))
                        for _, r in markers_df.iterrows()
                        if r.get("complex_id") is not None
                    }
                    preview_df["complex_id"] = preview_df["단지명"].map(name_to_id)
                    preview_df["수집"] = True
                    preview_markers_df = markers_df
                    candidate_ids = [int(v) for v in preview_df["complex_id"].dropna().tolist()]
                    st.warning("미리보기 API 버전이 달라 기본 모드로 후보를 생성했습니다.")
            except ValueError as exc:
                progress_bar.empty()
                progress_text.empty()
                st.error(str(exc))
                return
        preview_elapsed = __import__("time").perf_counter() - preview_started_at
        progress_bar.progress(100, text=f"후보 조회 완료 ({preview_elapsed:.1f}s)")
        progress_text.caption(f"후보 {len(candidate_ids)}개 확인, 소요 {preview_elapsed:.1f}s")
        st.session_state["preview_ready"] = True
        st.session_state["preview_df"] = preview_df
        preview_select_df = preview_df.copy()
        if "수집" in preview_select_df.columns:
            cols = ["수집"] + [c for c in preview_select_df.columns if c != "수집"]
            preview_select_df = preview_select_df[cols]
        else:
            preview_select_df.insert(0, "수집", True)
        st.session_state["preview_select_df"] = preview_select_df
        st.session_state["preview_markers_df"] = preview_markers_df
        st.session_state["candidate_ids"] = candidate_ids
        st.session_state["query"] = query.strip()
        st.session_state["dong"] = dong.strip()
        st.session_state["radius_m"] = float(radius_m)
        st.session_state["min_households"] = int(min_households)
        st.session_state["has_result"] = False

    if not st.session_state.get("preview_ready", False):
        st.caption("왼쪽에서 조건 입력 후 `후보 조회`를 눌러 후보 단지를 먼저 확인하세요.")
        return

    st.subheader("후보 단지 목록")
    hidden_preview_cols = ["complex_id", "seed_query", "is_seed"]
    preview_editor_df = st.session_state["preview_select_df"].drop(columns=hidden_preview_cols, errors="ignore")
    edited_preview_df = st.data_editor(
        preview_editor_df,
        use_container_width=True,
        hide_index=True,
        key="preview_editor",
        column_config={
            "수집": st.column_config.CheckboxColumn("수집", help="체크된 단지만 수집"),
        },
        disabled=[c for c in preview_editor_df.columns if c != "수집"],
    )
    if "수집" in edited_preview_df.columns:
        st.session_state["preview_select_df"].loc[edited_preview_df.index, "수집"] = edited_preview_df["수집"].astype(bool)

    selected_ids = (
        st.session_state["preview_select_df"].loc[st.session_state["preview_select_df"]["수집"] == True, "complex_id"]
        .astype(int)
        .tolist()
    )
    st.caption(f"선택된 후보: {len(selected_ids)}개")

    st.subheader("후보 단지 지도")
    map_markers_df = _enrich_marker_names(
        st.session_state["preview_markers_df"],
        st.session_state.get("preview_select_df"),
    )
    _render_map(map_markers_df, radius_m=float(st.session_state.get("radius_m", radius_m)))
    _render_adsense_slot(ADSENSE_SLOT_MID, height=120)

    collect_clicked = st.button("수집하기")
    if collect_clicked:
        if not selected_ids:
            st.warning("수집할 후보 단지를 1개 이상 선택해 주세요.")
            return
        progress_bar = st.progress(0, text="수집 준비 중...")
        progress_text = st.empty()
        with st.spinner("후보 단지 수집 중입니다..."):
            try:
                result_df, _selected_info, crawled_info, markers_df, _metrics = _call_collect_dataset_compat(
                    raw_query=st.session_state["query"],
                    radius_m=float(st.session_state["radius_m"]),
                    min_households=int(st.session_state["min_households"]),
                    progress_callback=_on_progress,
                    fast_mode=True,
                    max_dong_codes=None,
                    index_items=_get_kb_index_resilient(),
                    preferred_dong=(st.session_state.get("dong") or None),
                    candidate_ids=selected_ids,
                )
            except ValueError as exc:
                progress_bar.empty()
                progress_text.empty()
                st.error(str(exc))
                return
        progress_bar.progress(100, text="수집 완료")

        save_stem = _save_stem_from_query(st.session_state["query"])
        out_path = ae.save_output(result_df, query=save_stem)
        file_bytes = Path(out_path).read_bytes()
        st.session_state["has_result"] = True
        st.session_state["result_df"] = result_df
        st.session_state["markers_df"] = markers_df
        st.session_state["crawled_count"] = len(crawled_info)
        st.session_state["download_bytes"] = file_bytes
        st.session_state["download_name"] = Path(out_path).name

    if not st.session_state.get("has_result", False):
        return

    result_df = st.session_state["result_df"]
    markers_df = st.session_state["markers_df"]
    crawled_count = st.session_state["crawled_count"]
    st.success(f"수집 완료: {len(result_df)}행, 단지 {crawled_count}개")
    st.subheader("수집 결과")
    st.dataframe(
        _styled_result_df(result_df),
        use_container_width=True,
        hide_index=True,
        column_config={
            ae.COL_LINK: st.column_config.LinkColumn(
                "해당단지링크",
                help="KB 단지 페이지로 이동",
                display_text="열기",
            )
        },
    )

    st.download_button(
        label="엑셀 다운로드",
        data=st.session_state["download_bytes"],
        file_name=st.session_state["download_name"],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    _render_adsense_slot(ADSENSE_SLOT_BOTTOM, height=120)


if __name__ == "__main__":
    main()
