import pytest

from jobmon.workflow.abstract_task import AbstractTask


def test_good_names():
    assert AbstractTask.is_valid_task_name("fred")
    assert AbstractTask.is_valid_task_name("fred123")
    assert AbstractTask.is_valid_task_name("fred_and-friends")


def test_bad_names_without_raise():
    assert AbstractTask.is_valid_task_name("", raise_on_error=False) == (False, "name cannot be None or empty")
    assert AbstractTask.is_valid_task_name("/tmp/fred", raise_on_error=False) \
           == (False, "name contains illegal special character, illegal characters are: '{}'".
                    format(AbstractTask.ILLEGAL_SPECIAL_CHARACTERS))
    assert AbstractTask.is_valid_task_name("1fred", raise_on_error=False) == (
    False, "name cannot begin with a digit, saw: '1'")
    assert AbstractTask.is_valid_task_name("fred\_and", raise_on_error=False)\
           == (False, "name contains illegal special character, illegal characters are: '{}'".
                    format(AbstractTask.ILLEGAL_SPECIAL_CHARACTERS))


def test_bad_names_with_raise():
    with pytest.raises(ValueError) as exc:
        AbstractTask.is_valid_task_name("")
    assert "None" in str(exc.value)
