from __future__ import annotations

from pathlib import Path
import sys

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

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_df,
        get_position="[lng, lat]",
        get_radius=70,
        get_fill_color="color",
        pickable=True,
    )
    tooltip = {
        "html": "<b>{complex_name}</b><br/>id: {complex_id}<br/>seed: {is_seed}<br/>query: {seed_query}",
        "style": {"backgroundColor": "white", "color": "black"},
    }
    st.pydeck_chart(pdk.Deck(map_style="mapbox://styles/mapbox/light-v9", initial_view_state=view, layers=[layer], tooltip=tooltip))


def main() -> None:
    st.set_page_config(page_title="KB 부동산 크롤러", layout="wide")
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

    with st.spinner("데이터 수집 중입니다..."):
        try:
            result_df, selected_info, crawled_info, markers_df = collect_dataset(
                raw_query=query.strip(),
                radius_m=float(radius_m),
                min_households=int(min_households),
            )
        except ValueError as exc:
            st.error(str(exc))
            return

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

    st.subheader("단지 위치 지도")
    _render_map(markers_df)

    st.subheader("지도 마커 목록")
    st.dataframe(markers_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
