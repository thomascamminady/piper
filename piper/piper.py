"""Polars pipes for the performance management chart."""

import logging

import polars as pl
from polars.datatypes.classes import DataTypeClass
from polars.exceptions import ComputeError, InvalidOperationError


class Piper:
    @staticmethod
    def magic(_df: pl.DataFrame) -> pl.DataFrame:
        """
        Apply a series of transformations to the DataFrame.

        Parameters
        ----------
        _df : pl.DataFrame
            The input DataFrame.

        Returns
        -------
        pl.DataFrame
            The transformed DataFrame.

        """
        return (
            _df.pipe(Piper.drop_rows_that_are_all_null)
            .pipe(Piper.drop_columns_that_are_all_null)
            .pipe(Piper.utf8_promotion)
            .pipe(Piper.semicircle_to_degrees)
        )

    @staticmethod
    def drop_rows_that_are_all_null(_df: pl.DataFrame) -> pl.DataFrame:
        """
        Drop rows that are all null.

        Parameters
        ----------
        _df : pl.DataFrame
            The input DataFrame.

        Returns
        -------
        pl.DataFrame
            The DataFrame with rows that are all null removed.

        """
        return _df.filter(~pl.all_horizontal(pl.all().is_null()))

    @staticmethod
    def drop_columns_that_are_all_null(_df: pl.DataFrame) -> pl.DataFrame:
        """
        Drop columns that are all null.

        Parameters
        ----------
        _df : pl.DataFrame
            The input DataFrame.

        Returns
        -------
        pl.DataFrame
            The DataFrame with columns that are all null removed.

        """
        return _df[[s.name for s in _df if not (s.null_count() == _df.height)]]

    @staticmethod
    def semicircle_to_degrees(_df: pl.DataFrame) -> pl.DataFrame:
        """
        Convert semicircles to degrees.

        Parameters
        ----------
        _df : pl.DataFrame
            The input DataFrame.

        Returns
        -------
        pl.DataFrame
            The DataFrame with semicircles converted to degrees.

        """
        cols = [col for col in _df.columns if "_lat" in col or "_lon" in col]
        logging.info(f"Converting semicircles to degrees for columns: {cols}")
        return _df.with_columns(pl.col(cols) * 180 / 2**31)

    @staticmethod
    def convert_times_to_datetime(
        _df: pl.DataFrame, cols: list[str] | None = None
    ) -> pl.DataFrame:
        """
        Convert the times to datetime.

        Parameters
        ----------
        _df : pl.DataFrame
            The input DataFrame.
        cols : list of str, optional
            The columns to convert. If None, defaults to common time columns.

        Returns
        -------
        pl.DataFrame
            The DataFrame with times converted to datetime.

        """
        cols = cols or ["timestamp", "start_time", "created_at", "updated_at"]
        cols = [col for col in cols if col in _df.columns]
        logging.info(f"Converting times to datetime for columns: {cols}")
        return _df.with_columns(pl.col(cols).str.to_datetime())

    @staticmethod
    def cast_time_in_zone_string_to_list_of_float(
        _df: pl.DataFrame, cols: list[str] | None = None
    ) -> pl.DataFrame:
        """
        Cast the time in zone string to a list of floats.

        Parameters
        ----------
        _df : pl.DataFrame
            The input DataFrame.
        cols : list of str, optional
            The columns to convert. If None, defaults to common time zone columns.

        Returns
        -------
        pl.DataFrame
            The DataFrame with time in zone strings converted to lists of floats.

        """
        cols = cols or ["time_in_hr_zone_sec", "time_in_pwr_zone_sec"]
        cols = [col for col in cols if col in _df.columns]
        return _df.with_columns(
            pl.col(cols).str.split("|").cast(pl.List(pl.Float64))
        )

    @staticmethod
    def utf8_promotion(_df: pl.DataFrame) -> pl.DataFrame:
        """
        Try to promote string datatypes to datetimes, ints, or floats.

        Parameters
        ----------
        _df : pl.DataFrame
            The input DataFrame.

        Returns
        -------
        pl.DataFrame
            The DataFrame with promoted datatypes.

        """
        for col in _df.columns:
            if _df[col].dtype != pl.Utf8:
                continue

            _df, _success = Piper.try_to_datetime(_df, col)
            if _success:
                continue

            _df, _success = Piper.try_to_numeric(_df, col, pl.Int64)
            if _success:
                continue

            _df, _success = Piper.try_to_numeric(_df, col, pl.Float64)

        return _df

    @staticmethod
    def try_convert_dtypes_to_float_if_possible(
        _df: pl.DataFrame,
    ) -> pl.DataFrame:
        """
        Convert the data types to float if possible.

        Parameters
        ----------
        _df : pl.DataFrame
            The input DataFrame.

        Returns
        -------
        pl.DataFrame
            The DataFrame with data types converted to float where possible.

        """
        for col in _df.columns:
            try:
                _df = _df.with_columns(pl.col(col).cast(pl.Float64))
            except InvalidOperationError:
                pass

        return _df

    @staticmethod
    def try_to_datetime(
        _df: pl.DataFrame, col: str
    ) -> tuple[pl.DataFrame, bool]:
        """
        Try to convert the column to a datetime datatype.

        Parameters
        ----------
        _df : pl.DataFrame
            The input DataFrame.
        col : str
            The column to convert.

        Returns
        -------
        tuple[pl.DataFrame, bool]
            The DataFrame and a boolean indicating success.

        Examples
        --------
        >>> df = pl.DataFrame({"a": ["2023-01-01"]})
        >>> Piper.try_to_datetime(df, "a")
        """
        for format in [
            "%B %d, %Y, %I:%M %p",  # e.g. "February 22, 2023, 11:56 AM"
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%.fZ",  # iso format
        ]:
            try:
                _df = _df.with_columns(
                    pl.col(col).str.to_datetime(format=format)
                )
                return _df, True
            except InvalidOperationError:
                pass
            except ComputeError:
                pass
        return _df, False

    @staticmethod
    def try_to_numeric(
        _df: pl.DataFrame, col: str, dtype: DataTypeClass
    ) -> tuple[pl.DataFrame, bool]:
        """
        Try to convert the column to a numeric datatype.

        Parameters
        ----------
        _df : pl.DataFrame
            The input DataFrame.
        col : str
            The column to convert.
        dtype : DataTypeClass
            The target numeric datatype.

        Returns
        -------
        tuple[pl.DataFrame, bool]
            The DataFrame and a boolean indicating success.

        """
        try:
            _df = _df.with_columns(
                pl.col(col).str.replace_all(",", "").cast(dtype)
            )
            return _df, True
        except InvalidOperationError:
            return _df, False
        except ComputeError:
            return _df, False
