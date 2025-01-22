from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Optional, Any
from zoneinfo import ZoneInfo

import arrow
from arrow import Arrow
from attr import dataclass


@dataclass
class DominionCredentials:
    email_address: str
    password: str


@dataclass
class BillSummary:
    """Represents a Dominion Energy bill summary."""
    account_number: str
    previous_bill_period_start: Optional[Arrow]
    previous_bill_period_end: Optional[Arrow]
    next_meter_read_date: Arrow
    previous_balance: Decimal
    payments_received: Decimal
    remaining_balance: Decimal
    current_charges: Decimal
    total_account_balance: Decimal
    pending_payments: Decimal
    is_meter_read_estimated: bool = True  # Default to True since most reads are estimated
    timezone: ZoneInfo | None = None

    def update_timezone(self, new_timezone: ZoneInfo | None) -> None:
        self.timezone = new_timezone

@dataclass
class DownloadResult:
    filepath: Path = Path()
    timestamp: arrow.Arrow = arrow.now(tz="local")
    success: bool = False
    bill_summary: Optional[BillSummary] = None
    error: str | None = None

@dataclass
class LoginResult:
    """Result of a login attempt."""
    success: bool
    error: str | None = None
    balance: str | None = None # TODO probably not needed when saving

@dataclass(frozen=True)
class YearToDateMetrics:
    """Year to date usage metrics."""
    current_year: int
    total_usage_kwh: float
    total_cost: float
    days_elapsed: int
    daily_average_kwh: float
    comparison_years: dict[int, dict[str, float]]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for state attributes."""
        base_dict = {
            "current_year": self.current_year,
            "total_usage_kwh": self.total_usage_kwh,
            "total_cost": self.total_cost,
            "days_elapsed": self.days_elapsed,
            "daily_average_kwh": self.daily_average_kwh,
        }

        if self.comparison_years:
            base_dict.update({
                f"year_{year}_usage": data["usage"]
                for year, data in self.comparison_years.items()
            })
            base_dict.update({
                f"year_{year}_daily_average": data["daily_avg"]
                for year, data in self.comparison_years.items()
            })

        return base_dict

@dataclass(frozen=True)
class EnergyUsagePeriodComparison:
    """Comparison of energy usage between two billing periods."""
    current_kwh: float
    previous_kwh: float
    percent_change: float
    absolute_change_kwh: float
    days_current: int
    days_previous: int

    @classmethod
    def create(
            cls,
            current_kwh: float,
            previous_kwh: float,
            days_current: int,
            days_previous: int,
    ) -> "EnergyUsagePeriodComparison":
        """Create energy usage comparison with calculated fields."""
        absolute_change = current_kwh - previous_kwh
        percent_change = (
            (absolute_change / previous_kwh) * 100
            if previous_kwh != 0
            else 0.0
        )
        return cls(
            current_kwh=current_kwh,
            previous_kwh=previous_kwh,
            percent_change=percent_change,
            absolute_change_kwh=absolute_change,
            days_current=days_current,
            days_previous=days_previous,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for state attributes."""
        return {
            "current_kwh": self.current_kwh,
            "previous_kwh": self.previous_kwh,
            "percent_change": self.percent_change,
            "absolute_change_kwh": self.absolute_change_kwh,
            "days_current_period": self.days_current,
            "days_previous_period": self.days_previous,
            "current_daily_average_kwh": self.current_kwh / max(self.days_current, 1),
            "previous_daily_average_kwh": self.previous_kwh / max(self.days_previous, 1),
        }

@dataclass(frozen=True)
class BillingPeriodComparison:
    """Comparison of costs between two billing periods."""
    current_dollars: float
    previous_dollars: float
    percent_change: float
    absolute_change_dollars: float
    days_current: int
    days_previous: int

    @classmethod
    def create(
            cls,
            current_dollars: float,
            previous_dollars: float,
            days_current: int,
            days_previous: int,
    ) -> "BillingPeriodComparison":
        """Create billing cost comparison with calculated fields."""
        absolute_change = current_dollars - previous_dollars
        percent_change = (
            (absolute_change / previous_dollars) * 100
            if previous_dollars != 0
            else 0.0
        )
        return cls(
            current_dollars=current_dollars,
            previous_dollars=previous_dollars,
            percent_change=percent_change,
            absolute_change_dollars=absolute_change,
            days_current=days_current,
            days_previous=days_previous,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for state attributes."""
        return {
            "current_dollars": self.current_dollars,
            "previous_dollars": self.previous_dollars,
            "percent_change": self.percent_change,
            "absolute_change_dollars": self.absolute_change_dollars,
            "days_current_period": self.days_current,
            "days_previous_period": self.days_previous,
            "current_daily_average_dollars": self.current_dollars / max(self.days_current, 1),
            "previous_daily_average_dollars": self.previous_dollars / max(self.days_previous, 1),
        }

# region Electricity
# @dataclass(frozen=True)
# class PowerReading:
#     timestamp: Arrow
#     power_kw: float
#     energy_kwh: float
#
#     @property
#     def hour_of_day(self) -> int:
#         return self.timestamp.timestamp().hour


# class DailyComparison(NamedTuple):
#     date: Arrow
#     energy_kwh: float
#     average_energy_kwh: float
#
#     @property
#     def difference_from_average(self) -> float:
#         return self.energy_kwh - self.average_energy_kwh
#
#     @property
#     def percentage_difference(self) -> float:
#         return (self.difference_from_average / self.average_energy_kwh * 100
#                 if self.average_energy_kwh != 0 else 0.0)
#
#
# class HourlyStatistic(NamedTuple):
#     hour: int
#     average_energy_kwh: float
#     percentage_of_daily: float


# @dataclass
# class DailyUsage:
#     date: Arrow
#     readings: List[PowerReading]
#
#     @property
#     def total_energy_kwh(self) -> float:
#         """Calculate total energy usage for the day"""
#         return (
#             seq(self.readings)
#             .map(lambda reading: reading.energy_kwh)
#             .sum()
#         )
#
#     @property
#     def peak_power_kw(self) -> float:
#         """Find peak power usage for the day"""
#         return (
#             seq(self.readings)
#             .map(lambda reading: reading.power_kw)
#             .max()
#         )
#
#     @property
#     def average_power_kw(self):
#         return (
#             seq(self.readings)
#             .map(lambda reading: reading.power_kw)
#             .average()
#         )
#
#
# @dataclass(frozen=True)
# class UsageDataset:
#     daily_usages: List[DailyUsage]
#
#     @property
#     def total_energy_kwh(self) -> float:
#         """Calculate total energy usage across all days"""
#         return (
#             seq(self.daily_usages)
#             .map(lambda day: day.total_energy_kwh)
#             .sum()
#         )
#
#     def average_daily_energy_kwh(self) -> float:
#         return (
#             seq(self.daily_usages)
#             .map(lambda day: day.total_energy_kwh)
#             .average()
#         )
#
#     def analyze_daily_trends(self) -> List[DailyComparison]:
#         """
#         Analyze each day's usage compared to the overall average.
#         Useful for identifying unusually high or low usage days.
#         """
#         return (
#             seq(self.daily_usages)
#             .map(lambda day: DailyComparison(
#                 date=day.date,
#                 energy_kwh=day.total_energy_kwh,
#                 average_energy_kwh=self.average_daily_energy_kwh()
#             ))
#             .sorted(key=lambda day: abs(day.percentage_difference), reverse=True)
#             .to_list()
#         )
#
#     def create_hourly_statistics(self, hour: int, average: float,
#                                  total_daily: float) -> HourlyStatistic:
#         """Create statistics for a given hour"""
#         percentage = average / total_daily * 100 if total_daily > 0 else 0.0
#         return HourlyStatistic(
#             hour=hour,
#             average_energy_kwh=average,
#             percentage_of_daily=percentage
#         )
#
#     def analyze_hourly_patterns(self) -> List[HourlyStatistic]:
#         """
#         Analyze average energy usage by hour of day
#         Useful for identifying high usage hours and optimizing usage patterns
#         :return:
#         """
#         all_readings = (
#             seq(self.daily_usages)
#             .flat_map(lambda day: day.readings)
#         )
#
#         hourly_averages = (
#             all_readings
#             .group_by(lambda reading: reading.hour_of_day)
#             .map(lambda group:
#                  seq(group[1])
#                  .map(lambda reading: reading.energy_kwh)
#                  .average()
#                  )
#             .to_dict()
#         )
#
#         total_daily_average = sum(hourly_averages.values())
#         hours_in_day = 24
#
#         return (
#             seq(range(hours_in_day))
#             .map(lambda hour: self.create_hourly_statistics(
#                 hour=hour,
#                 average=hourly_averages.get(hour, 0.0),
#                 total_daily=total_daily_average
#             ))
#             .sorted(key=lambda statistic: statistic.average_energy_kwh, reverse=True)
#             .to_list()
#         )
#
#     def find_highest_usage_hours(self, top_n: int = 3) -> List[HourlyStatistic]:
#         """Find the hours with the highest average energy usage."""
#         return (
#             seq(self.analyze_hourly_patterns())
#             .take(top_n)
#             .to_list()
#         )
#
#     def find_abnormal_days(self, threshold_percent: float = 25.0) -> List[DailyComparison]:
#         """
#         Find days where usage was significantly different from average.
#
#         Args:
#          threshold_percent: How much deviation from average to consider abnormal
#         """
#         return (
#             seq(self.analyze_daily_trends())
#             .filter(lambda day: abs(day.percentage_difference) > threshold_percent)
#         )


# endregion
# region Billing
class BillingPeriod(Enum):
    SUMMER = "SUMMER"  # June - September
    NON_SUMMER = "NON_SUMMER"  # October - May

# @dataclass(frozen=True)
# class RateStructure:
#     """
#     Immutable rate structure containing all Dominion Energy Virginia residential rate components.
#     All rates are in dollars per kWh unless otherwise specified.
#     """
#
#     #region Basic charges
#     basic_customer_charge: float = 7.58  # Fixed monthly charge for having service ($)
#     first_800_kwh_distribution: float = 0.024986  # Distribution charge for first 800 kWh
#     over_800_kwh_distribution: float = 0.017955   # Distribution charge for usage above 800 kWh
#     #endregion
#
#     #region Generation rates - Base charges for electricity generation
#     first_800_kwh_generation: float = 0.029421    # Generation charge for first 800 kWh
#     over_800_kwh_generation_summer: float = 0.044768     # Generation charge for usage above 800 kWh in summer
#     over_800_kwh_generation_non_summer: float = 0.022706 # Generation charge for usage above 800 kWh in non-summer
#     #endregion
#
#     def get_over_800_generation_rate(self, billing_period: BillingPeriod) -> float:
#         """Get the over-800 kWh generation rate for the billing period"""
#         if billing_period == BillingPeriod.SUMMER:
#             return self.over_800_kwh_generation_summer
#         else:
#             return self.over_800_kwh_generation_non_summer
#
#     def get_total_rate_over_800(self, billing_period: BillingPeriod) -> float:
#         """Calculate total rate per kWh for usage over 800 kWh for given billing period"""
#         return (
#                 self.over_800_kwh_distribution +
#                 self.get_over_800_generation_rate(billing_period) +
#                 # Rest of the calculation remains the same
#                 self.transmission_charge +
#                 self.transmission_rider_t1 +
#                 self.fuel_rider_a +
#                 # ... other components
#                 0.0  # Placeholder for full calculation
#         )

# endregion
