import pytest

from jobmon.units import TimeUnit

tu_test_data = [
    (1, 0, 0, (1, 0.02, 0.0, '0:00:01')),
    (0, 1, 0, (60, 1.0, 0.02, '0:01:00')),
    (0, 0, 1, (3600, 60.0, 1.0, '1:00:00')),
    (10, 1, 1.5, (5470, 91.17, 1.52, '1:31:10')),
    (7, 5.25, 24, (86722, 1445.37, 24.09, '1 day, 0:05:22'))
]
@pytest.mark.parametrize("s, m, h, e", tu_test_data)
def test_timedistance_v0(s, m, h, e):
    tu = TimeUnit(sec=s, min=m, hour=h)
    assert tu.seconds == e[0]
    assert tu.minutes == e[1]
    assert tu.hours == e[2]
    assert tu.readable == e[3]
