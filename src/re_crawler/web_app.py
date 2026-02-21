from __future__ import annotations

import math
import sys
from pathlib import Path

import pydeck as pdk
import streamlit as st

# Streamlit Cloud runs this file directly, so ensure `src` is on sys.path.
SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from re_crawler.auto_excel import collect_dataset, save_output, split_queries


def _save_stem_from_query(raw_query: str) -> str:
    queries = split_queries(raw_query)
    if not queries:
        return "complex"
    return queries[0] if len(queries) == 1 else f"{queries[0]}_외{len(queries)-1}"


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
        query = st.text_input("단지명", value="백련산SK뷰아이파크")
        radius_m = st.number_input("반경(m)", min_value=100, max_value=5000, value=500, step=100)
        min_households = st.number_input("최소 세대수", min_value=1, max_value=10000, value=290, step=10)
        run_clicked = st.button("크롤링 실행", type="primary")
    if "has_result" not in st.session_state:
        st.session_state["has_result"] = False

    def _on_progress(event: dict) -> None:
        if event.get("event") == "query_target_ready":
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

    if run_clicked:
        if not query.strip():
            st.error("단지명을 입력해 주세요.")
            return

        progress_bar = st.progress(0, text="수집 준비 중...")
        progress_text = st.empty()
        with st.spinner("데이터 수집 중입니다..."):
            try:
                result_df, _selected_info, crawled_info, markers_df, _metrics = collect_dataset(
                    raw_query=query.strip(),
                    radius_m=float(radius_m),
                    min_households=int(min_households),
                    progress_callback=_on_progress,
                    fast_mode=True,
                    max_dong_codes=8,
                )
            except ValueError as exc:
                progress_bar.empty()
                progress_text.empty()
                st.error(str(exc))
                return
        progress_bar.progress(100, text="수집 완료")

        save_stem = _save_stem_from_query(query.strip())
        out_path = save_output(result_df, query=save_stem)
        file_bytes = Path(out_path).read_bytes()
        st.session_state["has_result"] = True
        st.session_state["result_df"] = result_df
        st.session_state["markers_df"] = markers_df
        st.session_state["crawled_count"] = len(crawled_info)
        st.session_state["download_bytes"] = file_bytes
        st.session_state["download_name"] = Path(out_path).name

    if not st.session_state.get("has_result", False):
        st.caption("왼쪽에서 단지명과 옵션을 입력한 뒤 `크롤링 실행`을 누르세요.")
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

    st.subheader("단지 위치 지도")
    _render_map(markers_df, radius_m=float(radius_m))


if __name__ == "__main__":
    main()
