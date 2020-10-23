import arrow

from misc.constant.value import DEFAULT_DATETIME_FORMAT


def get_current_datetime():
    return arrow.now().format(DEFAULT_DATETIME_FORMAT)


def str_to_datetime(str_dt):
    return arrow.get(str_dt).format(DEFAULT_DATETIME_FORMAT)


def add_second_to_datetime(dt, second):
    return arrow.get(dt).shift(seconds=second).format(DEFAULT_DATETIME_FORMAT)
