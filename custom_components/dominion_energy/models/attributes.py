"""Data classes for sensor attributes."""
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any

import polars as pl
from arrow import Arrow
from polars import DataFrame

from custom_components.dominion_energy.models import BillSummary


# Column names centralized to avoid repetition and typos
class Columns:
    """Column names used in DataFrames."""
    TIMESTAMP = "timestamp"
    POWER_KW = "power_kw"
    ENERGY_KWH = "energy_kwh"

    # Derived column names (from aggregations)
    TOTAL_ENERGY_KWH = "total_energy_kwh"
    AVG_DAILY_ENERGY_KWH = "avg_daily_energy_kwh"
    AVG_POWER_KW = "avg_power_kw"
    PEAK_POWER_KW = "peak_power_kw"

@dataclass
class PeakPower:
    """Peak power information."""
    value: float
    timestamp: datetime

@dataclass
class DailyUsage:
    """Daily energy usage information."""
    date: date
    total_energy_kwh: float
    avg_power_kw: float
    peak_power_kw: float


@dataclass
class DailyStats:
    """Daily power and energy statistics."""
    usage: DailyUsage
    peak_power: PeakPower
    average_power_kw: float
    data_points: int

    def day(self) -> date:
        return self.usage.date

    @classmethod
    def from_dataframe(cls, data: DataFrame) -> "DailyStats":
        """Create DailyStats from a DataFrame."""
        latest_date = data.get_column(Columns.TIMESTAMP).dt.date().max()
        daily_data = data.filter(pl.col(Columns.TIMESTAMP).dt.date() == latest_date)

        peak_value = daily_data.get_column(Columns.POWER_KW).max()
        peak_time = (
            daily_data
            .filter(pl.col(Columns.POWER_KW) == peak_value)
            .get_column(Columns.TIMESTAMP)
            .first()
        )

        usage = DailyUsage(
            date=latest_date,
            total_energy_kwh=daily_data.get_column(Columns.ENERGY_KWH).sum(),
            avg_power_kw=daily_data.get_column(Columns.POWER_KW).mean(),
            peak_power_kw=peak_value
        )

        return cls(
            usage=usage,
            peak_power=PeakPower(
                value=peak_value,
                timestamp=peak_time,
            ),
            average_power_kw=daily_data.get_column(Columns.POWER_KW).mean(),
            data_points=daily_data.height,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for HA state attributes."""
        # Create a flattened version for clearer attribute display
        flat_dict = {
            "date": self.day(),
            "total_energy_kwh": self.usage.total_energy_kwh,
            "avg_power_kw": self.usage.avg_power_kw,
            "peak_power_kw": self.usage.peak_power_kw,
            "peak_power_time": self.peak_power.timestamp,
            "data_points": self.data_points,
        }
        return flat_dict

@dataclass
class WeeklyAnalysis:
    """Weekly usage analysis."""
    total_energy_kwh: float
    avg_daily_energy_kwh: float
    daily_totals: list[DailyUsage]
    highest_usage: DailyUsage
    lowest_usage: DailyUsage

    @classmethod
    def from_dataframe(cls, data: DataFrame) -> "WeeklyAnalysis":
        """Create WeeklyAnalysis from a DataFrame."""

        daily_data = (
            data
            .group_by(pl.col(Columns.TIMESTAMP).dt.date())
            .agg([
                pl.sum(Columns.ENERGY_KWH).alias(Columns.TOTAL_ENERGY_KWH),
                pl.mean(Columns.POWER_KW).alias(Columns.AVG_POWER_KW),
                pl.max(Columns.POWER_KW).alias(Columns.PEAK_POWER_KW),
            ])
            .tail(7)
            .sort(Columns.TIMESTAMP)
        )
        weekly_total = daily_data.get_column(Columns.TOTAL_ENERGY_KWH).sum()
        avg_daily_energy_kwh = daily_data.get_column(Columns.TOTAL_ENERGY_KWH).mean()

        daily_usages = [
            DailyUsage(**row)
            for row in daily_data
            .select([
                pl.col(Columns.TIMESTAMP).alias("date"),
                pl.col(Columns.TOTAL_ENERGY_KWH),
                pl.col(Columns.AVG_POWER_KW),
                pl.col(Columns.PEAK_POWER_KW),
            ])
            .iter_rows(named=True)
        ]

        highest_row = DailyUsage(
            **daily_data
            .filter(pl.col(Columns.TOTAL_ENERGY_KWH) == pl.col(Columns.TOTAL_ENERGY_KWH).max())
            .select([
                pl.col(Columns.TIMESTAMP).alias("date"),
                pl.col(Columns.TOTAL_ENERGY_KWH),
                pl.col(Columns.AVG_POWER_KW),
                pl.col(Columns.PEAK_POWER_KW),
            ])
            .row(0, named=True)
        )


        lowest_row = DailyUsage(
            **daily_data
            .filter(pl.col(Columns.TOTAL_ENERGY_KWH) == pl.col(Columns.TOTAL_ENERGY_KWH).min())
            .select([
                pl.col(Columns.TIMESTAMP).alias("date"),
                pl.col(Columns.TOTAL_ENERGY_KWH),
                pl.col(Columns.AVG_POWER_KW),
                pl.col(Columns.PEAK_POWER_KW),
            ])
            .row(0, named=True)
        )

        return cls(
            total_energy_kwh=weekly_total,
            avg_daily_energy_kwh=avg_daily_energy_kwh,
            daily_totals=daily_usages,
            highest_usage=highest_row,
            lowest_usage=lowest_row
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for HA state attributes."""
        return asdict(self)

@dataclass
class BillingPeriodStats:
    start_date: date
    days_in_period: int
    total_energy_kwh: float
    daily_average_kwh: float
    daily_usages: list[DailyUsage]
    peak_day: DailyUsage
    peak_power: PeakPower

    @classmethod
    def from_dataframe(
            cls,
            data: DataFrame,
            bill: BillSummary,
    ) -> "BillingPeriodStats":
        now = Arrow.now().naive.replace(tzinfo=bill.timezone)
        data_unavailable_stat = cls(
            start_date=now,
            total_energy_kwh=0.0,
            daily_average_kwh=0.0,
            days_in_period=0,
            daily_usages=[],
            peak_day=DailyUsage(
                date=now,
                total_energy_kwh=0.0,
                avg_power_kw=0.0,
                peak_power_kw=0.0
            ),
            peak_power=PeakPower(
                value=0.0,
                timestamp=now
            )
        )

        if not bill.previous_bill_period_end:
            return data_unavailable_stat

        period_start = bill.previous_bill_period_end.shift(days=1).naive.replace(tzinfo=bill.timezone)
        period_end = bill.next_meter_read_date.naive.replace(tzinfo=bill.timezone)

        period_data = (
            data
            .filter(
                (pl.col(Columns.TIMESTAMP).dt.date() >= period_start.date()) &
                (pl.col(Columns.TIMESTAMP).dt.date() <= period_end.date())
            )
        )

        daily_data = (
            period_data
            .group_by(pl.col(Columns.TIMESTAMP).dt.date())
            .agg([
                pl.sum(Columns.ENERGY_KWH).alias(Columns.TOTAL_ENERGY_KWH),
                pl.mean(Columns.POWER_KW).alias(Columns.AVG_POWER_KW),
                pl.max(Columns.POWER_KW).alias(Columns.PEAK_POWER_KW),
            ])
            .sort(Columns.TIMESTAMP)
        )

        # Create list of DailyUsage objects
        daily_usages = [
            DailyUsage(
                date=row[Columns.TIMESTAMP],
                total_energy_kwh=row[Columns.TOTAL_ENERGY_KWH],
                avg_power_kw=row[Columns.AVG_POWER_KW],
                peak_power_kw=row[Columns.PEAK_POWER_KW]
            )
            for row in daily_data.iter_rows(named=True)
        ]

        peak_power_value = period_data.get_column(Columns.POWER_KW).max()
        peak_power_timestamp = (
            period_data
            .filter(pl.col(Columns.POWER_KW) == peak_power_value)
            .get_column(Columns.TIMESTAMP)
            .first()
        )

        # Find the day with the highest energy usage
        peak_day_data = (
            daily_data
            .filter(
                pl.col(Columns.TOTAL_ENERGY_KWH) ==
                daily_data.get_column(Columns.TOTAL_ENERGY_KWH).max()
            )
            .row(0, named=True)
        )

        peak_day = DailyUsage(
            date=peak_day_data[Columns.TIMESTAMP],
            total_energy_kwh=peak_day_data[Columns.TOTAL_ENERGY_KWH],
            avg_power_kw=peak_day_data[Columns.AVG_POWER_KW],
            peak_power_kw=peak_day_data[Columns.PEAK_POWER_KW]
        )

        # Calculate period totals
        total_energy = daily_data.get_column(Columns.TOTAL_ENERGY_KWH).sum()
        days = len(daily_usages)
        days_in_period = (period_end - period_start).days

        return cls(
            start_date=period_start,
            total_energy_kwh=total_energy,
            daily_average_kwh=total_energy / days if days > 0 else 0.0,
            days_in_period=days_in_period,
            daily_usages=daily_usages,
            peak_day=peak_day,
            peak_power=PeakPower(
                value=peak_power_value,
                timestamp=peak_power_timestamp,
            )
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for HA state attributes."""
        return {
            "start_date": self.start_date.isoformat(),
            "total_energy_kwh": self.total_energy_kwh,
            "daily_average_kwh": self.daily_average_kwh,
            "days_in_period": self.days_in_period,
            "peak_power_kw": self.peak_power.value,
            "peak_power_time": self.peak_power.timestamp.isoformat(),
            "peak_day_date": self.peak_day.date.isoformat(),
            "peak_day_energy_kwh": self.peak_day.total_energy_kwh,
            "daily_usages": [
                {
                    "date": day.date.isoformat(),
                    "total_energy_kwh": day.total_energy_kwh,
                    "avg_power_kw": day.avg_power_kw,
                    "peak_power_kw": day.peak_power_kw
                }
                for day in self.daily_usages
            ]
        }
