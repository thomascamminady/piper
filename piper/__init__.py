"""
.. include:: ../README.md
"""
# FILE: piper/__init__.py

from piper.piper import Piper

drop_rows_that_are_all_null = Piper.drop_rows_that_are_all_null
drop_columns_that_are_all_null = Piper.drop_columns_that_are_all_null
semicircle_to_degrees = Piper.semicircle_to_degrees
convert_times_to_datetime = Piper.convert_times_to_datetime
cast_time_in_zone_string_to_list_of_float = (
    Piper.cast_time_in_zone_string_to_list_of_float
)
utf8_promotion = Piper.utf8_promotion
try_convert_dtypes_to_float_if_possible = (
    Piper.try_convert_dtypes_to_float_if_possible
)
try_to_datetime = Piper.try_to_datetime
try_to_numeric = Piper.try_to_numeric
magic = Piper.magic


__all__ = [
    "Piper",
    "drop_rows_that_are_all_null",
    "drop_columns_that_are_all_null",
    "semicircle_to_degrees",
    "convert_times_to_datetime",
    "cast_time_in_zone_string_to_list_of_float",
    "utf8_promotion",
    "try_convert_dtypes_to_float_if_possible",
    "try_to_datetime",
    "try_to_numeric",
    "magic",
]
