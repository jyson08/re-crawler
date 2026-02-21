from __future__ import annotations

import math
import sys
import inspect
from pathlib import Path

import pydeck as pdk
import streamlit as st

# Streamlit Cloud runs this file directly, so ensure `src` is on sys.path.
SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import re_crawler.auto_excel as ae


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
    return ae.fetch_kb_complex_index(session)


def _build_label_text(row) -> str:
    built = row.get("built_year")
    hh = row.get("households")
    parking = row.get("parking")
    hall = row.get("hall_type")

    built_text = f"준공:{built}" if built is not None else "준공:-"
    hh_text = f"세대:{hh:,}" if isinstance(hh, (int, float)) else "세대:-"
    parking_text = f"주차:{parking}" if parking else "주차:-"
    hall_text = f"현관:{hall}" if hall else "현관:-"
    return f"{row['complex_name']}\n{built_text} | {hh_text}\n{parking_text} | {hall_text}"


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
        st.info("지도에 표시할 좌표 데이터가 없습니다.")
        return

    view = pdk.ViewState(
        latitude=float(markers_df["lat"].mean()),
        longitude=float(markers_df["lng"].mean()),
        zoom=13,
        pitch=0,
    )

    map_df = markers_df.copy()
    map_df["color"] = map_df["is_seed"].map(lambda x: [220, 53, 69, 180] if bool(x) else [52, 152, 219, 170])
    map_df["label_text"] = map_df.apply(_build_label_text, axis=1)
    seed_df = map_df[map_df["is_seed"] == True].copy()
    radius_dash_rows = []
    if not seed_df.empty:
        seed_df["radius_paths"] = seed_df.apply(
            lambda r: _circle_dashed_paths(float(r["lat"]), float(r["lng"]), float(radius_m)),
            axis=1,
        )
        seed_df["radius_label"] = f"반경: {int(radius_m)}m"
        for _, row in seed_df.iterrows():
            for path in row["radius_paths"]:
                radius_dash_rows.append({"path": path})

    marker_layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_df,
        get_position="[lng, lat]",
        get_radius=28,
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
        get_color=[33, 33, 33, 230],
        get_size=13,
        size_units="pixels",
        get_text_anchor="start",
        get_alignment_baseline="top",
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
            "준공연도: {built_year}<br/>"
            "세대수: {households}<br/>"
            "주차대수: {parking}<br/>"
            "현관구조: {hall_type}"
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
    st.title("KB 부동산 단지 크롤링")

    with st.sidebar:
        st.subheader("수집 옵션")
        dong = st.text_input("동(읍/면/동)", value="")
        st.caption("동명이인 단지가 많으면 동명을 먼저 입력하세요. 예: 응암동")
        query = st.text_input("단지명", value="백련산SK뷰아이파크")
        radius_m = st.number_input("반경(m)", min_value=100, max_value=5000, value=500, step=100)
        min_households = st.number_input("최소 세대수", min_value=1, max_value=10000, value=290, step=10)
        preview_clicked = st.button("후보 조회", type="primary")
    if "has_result" not in st.session_state:
        st.session_state["has_result"] = False
    has_preview_api = hasattr(ae, "preview_candidates")

    def _on_progress(event: dict) -> None:
        if event.get("event") == "prepare":
            stage = event.get("stage")
            msg = event.get("message") or "준비 중..."
            if stage == "index_start":
                progress_bar.progress(5, text=msg)
            elif stage == "index_done":
                progress_bar.progress(12, text=msg)
            else:
                progress_bar.progress(15, text=msg)
            progress_text.caption(msg)
        elif event.get("event") == "query_target_ready":
            total = int(event.get("total") or 0)
            q = event.get("query")
            progress_text.info(f"[{q}] 대상 단지 {total}개 확인")
            if total <= 0:
                progress_bar.progress(100, text=f"[{q}] 처리할 단지가 없습니다.")
            else:
                progress_bar.progress(0, text=f"[{q}] 0/{total} 처리 중")
        elif event.get("event") == "query_progress":
            total = max(1, int(event.get("total") or 1))
            current = min(total, int(event.get("current") or 0))
            q = event.get("query")
            name = event.get("complex_name") or ""
            pct = int((current / total) * 100)
            progress_bar.progress(pct, text=f"[{q}] {current}/{total} 처리 중")
            progress_text.caption(f"현재 단지: {name}")

    if preview_clicked:
        if not query.strip():
            st.error("단지명을 입력해 주세요.")
            return

        progress_bar = st.progress(0, text="수집 준비 중...")
        progress_text = st.empty()
        with st.spinner("데이터 수집 중입니다..."):
            try:
                progress_bar.progress(3, text="단지 인덱스 캐시 확인 중...")
                index_items = _cached_kb_index()
                if has_preview_api:
                    preview_df, preview_markers_df, candidate_ids, _selected = ae.preview_candidates(
                        raw_query=query.strip(),
                        radius_m=float(radius_m),
                        min_households=int(min_households),
                        fast_mode=True,
                        max_dong_codes=None,
                        index_items=index_items,
                        preferred_dong=(dong.strip() if dong.strip() else None),
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
        progress_bar.progress(100, text="후보 조회 완료")
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
    _render_map(st.session_state["preview_markers_df"], radius_m=float(st.session_state.get("radius_m", radius_m)))

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
                    index_items=_cached_kb_index(),
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
    st.dataframe(result_df, use_container_width=True, hide_index=True)

    st.download_button(
        label="엑셀 다운로드",
        data=st.session_state["download_bytes"],
        file_name=st.session_state["download_name"],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    main()
