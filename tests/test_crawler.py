from datetime import date

from re_crawler.parser import calc_age, calc_lease_ratio, calc_pyung, parse_korean_price_to_manwon


def test_parse_korean_price_to_manwon():
    assert parse_korean_price_to_manwon("12억 5000") == 125000
    assert parse_korean_price_to_manwon("9억") == 90000
    assert parse_korean_price_to_manwon("12억~13억") == 125000
    assert parse_korean_price_to_manwon(None) is None


def test_calc_helpers():
    assert calc_pyung(84.96) == 25.7
    assert calc_age(2010, today=date(2026, 2, 20)) == 16
    assert calc_lease_ratio(70000, 100000) == 70.0
