"""
.. include:: ../README.md
"""
# FILE: piper/__init__.py

from polarspiper.polarspiper import PolarsPiper

drop_rows_that_are_all_null = PolarsPiper.drop_rows_that_are_all_null
drop_columns_that_are_all_null = PolarsPiper.drop_columns_that_are_all_null
semicircle_to_degrees = PolarsPiper.semicircle_to_degrees
convert_times_to_datetime = PolarsPiper.convert_times_to_datetime
cast_time_in_zone_string_to_list_of_float = (
    PolarsPiper.cast_time_in_zone_string_to_list_of_float
)
utf8_promotion = PolarsPiper.utf8_promotion
try_convert_dtypes_to_float_if_possible = (
    PolarsPiper.try_convert_dtypes_to_float_if_possible
)
try_to_datetime = PolarsPiper.try_to_datetime
try_to_numeric = PolarsPiper.try_to_numeric
magic = PolarsPiper.magic


__all__ = [
    "PolarsPiper",
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
