from re_crawler.auto_excel import _format_date_floor, split_queries


def test_split_queries_keeps_comma_inside_parentheses():
    raw = "무지개마을(신한,건영),백련산SK뷰아이파크"
    assert split_queries(raw) == ["무지개마을(신한,건영)", "백련산SK뷰아이파크"]


def test_split_queries_single_with_parentheses():
    raw = "무지개마을(신한,건영)"
    assert split_queries(raw) == ["무지개마을(신한,건영)"]


def test_format_date_floor():
    assert _format_date_floor("20251210", "2") == "25.12.10/2층"
    assert _format_date_floor(None, "11") == "11층"
    assert _format_date_floor("20251210", None) == "25.12.10"
