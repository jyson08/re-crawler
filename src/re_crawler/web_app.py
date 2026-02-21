from __future__ import annotations

import sys
from pathlib import Path

import pydeck as pdk
import streamlit as st

# Streamlit Cloud runs this file directly, so ensure `src` is on sys.path.
SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from re_crawler.auto_excel import collect_dataset, save_output, save_query_metrics, split_queries


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


def _render_map(markers_df):
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

    marker_layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_df,
        get_position="[lng, lat]",
        get_radius=45,
        get_fill_color="color",
        pickable=True,
    )
    text_layer = pdk.Layer(
        "TextLayer",
        data=map_df,
        get_position="[lng, lat]",
        get_text="label_text",
        get_color=[33, 33, 33, 230],
        get_size=12,
        get_text_anchor="start",
        get_alignment_baseline="bottom",
        get_pixel_offset=[10, 6],
        pickable=False,
    )

    tooltip = {
        "html": (
            "<b>{complex_name}</b><br/>"
            "준공연도: {built_year}<br/>"
            "세대수: {households}<br/>"
            "주차대수: {parking}<br/>"
            "현관구조: {hall_type}<br/>"
            "id: {complex_id}<br/>"
            "seed: {is_seed}<br/>"
            "query: {seed_query}"
        ),
        "style": {"backgroundColor": "white", "color": "black"},
    }

    st.pydeck_chart(
        pdk.Deck(
            map_provider="carto",
            map_style="light",
            initial_view_state=view,
            layers=[marker_layer, text_layer],
            tooltip=tooltip,
        )
    )


def main() -> None:
    st.set_page_config(page_title="KB 부동산 단지 크롤링", layout="wide")
    st.title("KB 부동산 단지 크롤링")

    with st.sidebar:
        st.subheader("수집 옵션")
        query = st.text_input("단지명", value="백련산SK뷰아이파크")
        radius_m = st.number_input("반경(m)", min_value=100, max_value=5000, value=1000, step=100)
        min_households = st.number_input("최소 세대수", min_value=1, max_value=10000, value=290, step=10)
        run_clicked = st.button("크롤링 실행", type="primary")

    if not run_clicked:
        st.caption("왼쪽에서 단지명과 옵션을 입력한 뒤 `크롤링 실행`을 누르세요.")
        return

    if not query.strip():
        st.error("단지명을 입력해 주세요.")
        return

    progress_bar = st.progress(0, text="수집 준비 중...")
    progress_text = st.empty()

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

    with st.spinner("데이터 수집 중입니다..."):
        try:
            result_df, selected_info, crawled_info, markers_df, metrics = collect_dataset(
                raw_query=query.strip(),
                radius_m=float(radius_m),
                min_households=int(min_households),
                progress_callback=_on_progress,
            )
        except ValueError as exc:
            progress_bar.empty()
            progress_text.empty()
            st.error(str(exc))
            return
    progress_bar.progress(100, text="수집 완료")

    st.success(f"수집 완료: {len(result_df)}행, 단지 {len(crawled_info)}개")

    selected_rows = [
        {"입력쿼리": q, "선택단지ID": c.complex_id, "선택단지명": c.name, "유사도점수": round(c.score, 1)}
        for q, c in selected_info
    ]
    st.subheader("선택된 시드 단지")
    st.dataframe(selected_rows, use_container_width=True, hide_index=True)

    st.subheader("수집 결과")
    st.dataframe(result_df, use_container_width=True, hide_index=True)

    save_stem = _save_stem_from_query(query.strip())
    out_path = save_output(result_df, query=save_stem)
    file_bytes = Path(out_path).read_bytes()
    st.download_button(
        label="엑셀 다운로드",
        data=file_bytes,
        file_name=Path(out_path).name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    st.caption(f"서버 저장 경로: `{out_path}`")
    metric_path = save_query_metrics(metrics)
    st.caption(f"쿼리 메트릭 로그: `{metric_path}`")

    metric_rows = [
        {
            "query": m.query,
            "elapsed_sec": m.elapsed_sec,
            "seed_complex_id": m.seed_complex_id,
            "seed_complex_name": m.seed_complex_name,
            "nearby_candidates": m.nearby_candidates,
            "target_candidates": m.target_candidates,
            "attempted_complexes": m.attempted_complexes,
            "success_complexes": m.success_complexes,
            "failed_complexes": m.failed_complexes,
            "failure_rate_pct": m.failure_rate_pct,
        }
        for m in metrics
    ]
    st.subheader("쿼리 처리 로그")
    st.dataframe(metric_rows, use_container_width=True, hide_index=True)

    st.subheader("단지 위치 지도")
    _render_map(markers_df)


if __name__ == "__main__":
    main()
