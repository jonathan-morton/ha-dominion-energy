import asyncio
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import polars.selectors as cs
from attr import dataclass
from polars import DataFrame

from custom_components.dominion_energy import LOGGER
from custom_components.dominion_energy.models import BillSummary
from custom_components.dominion_energy.models.attributes import Columns


@dataclass(frozen=True)
class RawUsageData:
    power_df: pl.DataFrame
    energy_df: pl.DataFrame

class DominionDataProcessor:
    tzinfo: ZoneInfo

    def __init__(
            self,
            file_path: Path,
            timezone: str
    ):
        self.file_path = file_path
        self.timezone = timezone
        self.tzinfo = ZoneInfo(timezone)

    async def _validate_excel_sheets(self) -> dict[str, DataFrame]:
        try:
            sheets: dict[str, DataFrame] = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: pl.read_excel(self.file_path, sheet_id=0)
            )
            sheet_names = list(sheets.keys())

            if len(sheet_names) < 2:
                raise ValueError(
                    f"Excel file missing expected sheets. Found: {sheet_names}"
                )
            return sheets

        except Exception as exception:
            raise IOError(f"Failed to read Excel file {self.file_path}: {str(exception)}")


    async def _read_excel_sheets(self) -> RawUsageData:
        """
        Reads power and energy sheets from Excel file
        :return: RawUsage data containing both data frames
        :raise: Exception if file cannot be read or expected sheets not found.
        """
        try:
            sheets = await self._validate_excel_sheets()

            date_column = (
                pl.col("Date")
                .str.to_date(format="%m/%d/%Y")
            )
            total_column = (
                pl.sum_horizontal(cs.contains("kW")).alias("Total")
            )

            energy_df: DataFrame = (
                sheets.get("kWH Usage Data")
                .with_columns(
                    date_column,
                    total_column
                )
            )
            power_df: DataFrame = (
                sheets.get("kW Usage Data")
                .with_columns(
                    date_column,
                    total_column
                )
            )

            return RawUsageData(
                power_df=power_df,
                energy_df=energy_df
            )


        except Exception as exception:
            print(f"Failed to read Excel file {self.file_path}: {str(exception)}")
            raise

    def _clean(self, data: RawUsageData)-> RawUsageData:
        def drop_incomplete_dates(df: DataFrame)-> DataFrame:
            """Drop the latest date if it's incomplete (all PM values are zero)"""
            latest_date = df.select(pl.col("Date").max()).item()

            is_incomplete = (
                df
                .filter(pl.col("Date") == pl.col("Date").max())  # Get latest date
                .select(cs.contains(" PM"))  # Select PM columns Afternoon times will not be zero when full data is out
                .sum_horizontal().item() == 0
            )


            return (
                df.filter(pl.col("Date") != latest_date)
                if is_incomplete
                else df
            )
        try:
            clean_power_df = (
                data.power_df
                .pipe(drop_incomplete_dates)
            )

            clean_energy_df = (
                data.energy_df
                .pipe(drop_incomplete_dates)
            )

            return RawUsageData(
                power_df=clean_power_df,
                energy_df=clean_energy_df
            )
        except Exception as exception:
            print(f"Error cleaning data: {str(exception)}")
        raise

    def _transform_to_long_format(self, df: DataFrame, value_col_name: str) -> DataFrame:
        """
        Convert wide format (columns for each time) to long format with timestamp column.
        Args:
            df: Wide format DataFrame with DST-aware dates
            value_col_name: Name for the value column (e.g. 'power_kw' or 'energy_kwh')
        """
        try:
            # Get time columns (excluding Date and Total)
            time_cols = df.select(
                cs.contains("AM", "PM")
            ).columns

            return (
                df
                .unpivot(
                    index=["Date"],
                    on=time_cols,
                    variable_name="time_str",
                    value_name=value_col_name
                )
                .with_columns([
                    # Extract time from column name (e.g. '12:00 AM' from '12:00 AM kW')
                    pl.col("time_str").str.extract(r"(\d+:\d+ [AP]M)").alias("time"),
                ])
                .with_columns([
                    # Combine and parse timestamp - Date already has timezone info
                    pl.concat_str(["Date", "time"], separator=" ")
                    .str.strptime(pl.Datetime, format="%Y-%m-%d %I:%M %p")
                    .alias("timestamp")
                ])
                .drop(["Date", "time_str", "time"])
                .sort("timestamp")
            )

        except Exception as exception:
            LOGGER.error("Error transforming to long format: %s", str(exception))
            raise

    def _handle_dst(self, df: DataFrame) -> DataFrame:
        """Handle DST for a DataFrame with complete timestamps."""
        try:
            dst_df = (
                df
                .with_columns([
                    pl.col("timestamp")
                    .dt.replace_time_zone(
                        self.timezone,
                        ambiguous="earliest",  # Fall DST - use first occurrence
                        non_existent="null"  # Spring DST - mark missing hour as null
                    )
                    .alias("timestamp")
                ])
            )

            # Log any missing timestamps from DST
            if dst_df.get_column("timestamp").null_count() > 0:
                missing_times = (
                    dst_df
                    .filter(pl.col("timestamp").is_null())
                    .select(["timestamp"])
                )
                LOGGER.debug(f"Missing timestamps during DST transitions:\n{missing_times}")

            final_df = dst_df.filter(pl.col("timestamp").is_not_null())

            if final_df.height != df.height:
                LOGGER.warning(
                    f"Some timestamps were lost during DST transition handling. Original rows: {df.height}, Final rows: {final_df.height}"
                )

            return final_df

        except Exception as exception:
            LOGGER.error(f"Error handling DST transitions: {str(exception)}")
            raise

    async def process_for_entities(self) -> DataFrame:
        """
        Process data into format suitable for Home Assistant entities
        Returns DataFrame with columns: timestamp, power_kw, energy_kwh
        """
        try:
            raw_data = await self._read_excel_sheets()
            clean_data = self._clean(raw_data)

            # Transform both power and energy data to long format
            power_long = self._transform_to_long_format(
                clean_data.power_df,
                "power_kw"
            )
            energy_long = self._transform_to_long_format(
                clean_data.energy_df,
                "energy_kwh"
            )

            # Join power and energy data
            joined_df = (
                power_long
                .join(
                    energy_long,
                    on="timestamp",
                    how="inner"
                )
                .select([
                    "timestamp",
                    "power_kw",
                    "energy_kwh"
                ])
            )

            dst_df = self._handle_dst(joined_df)

            return dst_df

        except Exception as exception:
            print(f"Failed to process data for entities: {str(exception)}")
            raise

    def process_for_statistics(self, data: DataFrame) -> DataFrame:
        """
        Process data for statistics - resamples to hourly aligned intervals.
        Combines both the :00 and :30 readings into a single hourly value.
        """

        def _validate_hourly_aggregation(hourly_df: DataFrame, full_df: DataFrame) -> None:
            """
            Validate that hourly aggregation preserves total energy.
            Compares sum before and after aggregation to ensure no data is lost.
            """
            original_sum = (
                full_df
                .get_column(Columns.ENERGY_KWH)
                .sum()
            )

            hourly_sum = (
                hourly_df
                .get_column(Columns.ENERGY_KWH)
                .sum()
            )

            # Allow for small floating point differences
            if abs(original_sum - hourly_sum) > 0.001:
                raise ValueError(
                    f"Energy sum mismatch after hourly aggregation. "
                    f"Original: {original_sum:.3f} kWh, "
                    f"Hourly: {hourly_sum:.3f} kWh"
                )

        try:
            # Calculate hourly statistics
            hourly_data = (
                data
                .with_columns([
                    # Floor to nearest hour to group both :00 and :30 readings
                    pl.col(Columns.TIMESTAMP)
                    .dt.truncate("1h")
                    .alias(Columns.TIMESTAMP)
                ])
                .group_by(Columns.TIMESTAMP)
                .agg([
                    (pl.mean(Columns.POWER_KW)).alias(Columns.POWER_KW),
                    # Sum energy readings from both intervals in the hour
                    pl.sum(Columns.ENERGY_KWH).alias(Columns.ENERGY_KWH)
                ])
                .sort(Columns.TIMESTAMP)
            )

            # Validate the aggregation
            _validate_hourly_aggregation(hourly_df=hourly_data, full_df=data)

            return hourly_data

        except Exception as exception:
            LOGGER.error(f"Failed to process data for statistics: {str(exception)}")
            raise

    def update_bill_summary(self, bill_summary: BillSummary) -> BillSummary:
        bill_summary.timezone = self.tzinfo
        return bill_summary


