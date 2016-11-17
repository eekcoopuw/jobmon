
import pytest
from .mock_job import MockJob


def test_good_job():
    job = MockJob("good dog", 1, None)
    assert(job.run())


def test_bad_job():
    with pytest.raises(Exception) as exc:
        job = MockJob("bad dog", 1, "chew slippers")
        job.run()
    assert("chew slippers" in str(exc.value))

#
# Can't test this case, it deliberately kills python!
# def test_dead_job():
#     with pytest.raises(Exception) as exc:
#         job = MockJob("dead dog", 1, "die job")
#         job.run()
#     assert( "chew slippers" in str(exc.value))
