from collections.abc import Mapping
from datetime import timedelta, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from arrow import Arrow
from functional import seq
from homeassistant.components.recorder.models import StatisticData, StatisticMetaData
from homeassistant.components.recorder.statistics import get_last_statistics, statistics_during_period, \
    async_add_external_statistics
from homeassistant.const import CONF_USERNAME, CONF_PASSWORD, UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.recorder import get_instance
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
import polars as pl

from custom_components.dominion_energy.DominionDataProcessor import DominionDataProcessor
from custom_components.dominion_energy.api.DominionScraper import DominionScraper
from custom_components.dominion_energy.const import LOGGER, DOMAIN
from custom_components.dominion_energy.exceptions import InvalidAuth
from custom_components.dominion_energy.models import DominionCredentials, BillSummary
from custom_components.dominion_energy.models.attributes import Columns


class DominionEnergyUpdateCoordinator(DataUpdateCoordinator[pl.DataFrame]):
    """Coordinator to manage fetching data from Dominion Energy"""

    def __init__(
            self,
            hass: HomeAssistant,
            entry_data: Mapping[str, Any],
            download_dir: Path,
            driver_path: str,
    ):
        super().__init__(
            hass=hass,
            logger=LOGGER,
            name=DOMAIN,
            update_interval=timedelta(hours=12)
            # Data only updates once a day but may be incomplete in the early morning
        )
        self._download_dir = download_dir
        self._credentials = DominionCredentials(
            email_address=entry_data[CONF_USERNAME],
            password=entry_data[CONF_PASSWORD]
        )
        self._scraper = DominionScraper(
            credentials=self._credentials,
            download_directory=download_dir,
            driver_path=driver_path,
        )
        self.bill_summary: BillSummary | None = None
        self._last_data: pl.DataFrame | None = None

    async def _async_update_data(self) -> pl.DataFrame:
        """Fetch data from Dominion Energy"""
        downloaded_file: Path | None = None

        try:
            timezone = str(self.hass.config.time_zone)
            tzinfo = ZoneInfo(timezone)

            download_result = await self._scraper.fetch_usage_data(tzinfo)

            if not download_result.success:
                raise RuntimeError(f"Failed to fetch data: {download_result.error}")

            downloaded_file = download_result.filepath
            processor = DominionDataProcessor(
                file_path=download_result.filepath,
                timezone=timezone
            )
            data = await processor.process_for_entities()

            self.bill_summary = download_result.bill_summary
            account_id = download_result.bill_summary.account_number

            await self._insert_statistics(data, account_id, processor, timezone)
            self._last_data = data
            return data

        except InvalidAuth as exception:
            raise ConfigEntryAuthFailed from exception
        except Exception as exception:
            LOGGER.error("Error fetching Dominion Energy data: %s", str(exception))
            raise
        finally:
            if downloaded_file and downloaded_file.exists():
                try:
                    downloaded_file.unlink()
                    LOGGER.debug("Cleaned up downloaded file: %s", downloaded_file)
                except Exception as exception:
                    LOGGER.warning(
                        "Failed to cleanup downloaded file %s: %s",
                        downloaded_file,
                        str(exception)
                    )

    async def _insert_statistics(
            self,
            data: pl.DataFrame,
            account_id: str,
            processor: DominionDataProcessor,
            timezone: str
    ):
        """Insert energy usage statistics into Home Assistant."""

        statistic_id = f"{DOMAIN}:{account_id}_energy_consumption"
        LOGGER.warning(f"Time zone: {timezone}")

        hourly_df = processor.process_for_statistics(data)

        # Get last statistics for this statistic ID
        last_stat = await get_instance(self.hass).async_add_executor_job(
            get_last_statistics, self.hass, 1, statistic_id, True, set()
        )

        if last_stat and statistic_id in last_stat and last_stat[statistic_id]:
            try:
                last_stat_time = last_stat[statistic_id][0]["start"]
                last_stat_sum = last_stat[statistic_id][0]["sum"]
                LOGGER.debug(
                    "Last statistic time: %s, Last sum: %s",
                    last_stat_time,
                    last_stat_sum
                )
            except (KeyError, IndexError) as exception:
                LOGGER.warning(
                    "Failed to get last statistics info: %s",
                    str(exception)
                )
                last_stat_time = None
                last_stat_sum = 0.0
        else:
            LOGGER.debug("No previous statistics found for %s", statistic_id)
            last_stat_time = None
            last_stat_sum = 0.0

        async def get_and_convert_timestamp(timestamp_unix: float) -> datetime:
            """Convert unix timestamp to datetime with timezone in executor."""
            def _convert():
                naive_dt = Arrow.fromtimestamp(timestamp_unix, timezone).naive
                return naive_dt.replace(tzinfo=ZoneInfo(timezone))

            return await self.hass.async_add_executor_job(_convert)

        async def get_thirty_days_ago() -> datetime:
            """Get timestamp from 30 days ago in executor."""
            def _convert():
                # Get naive datetime from Arrow
                dt = (
                    Arrow.now()
                    .floor('day')
                    .shift(days=-30)
                    .naive
                )
                # Attach timezone explicitly
                return dt.replace(tzinfo=ZoneInfo(timezone))

            return await self.hass.async_add_executor_job(_convert)

        # Get the timestamp we should start from
        start_time_unix: float | None = None if not last_stat else last_stat[statistic_id][0]["start"]

        if start_time_unix is None:
            # First time processing - use all data
            usage_data = hourly_df
            base_sum = 0.0
            LOGGER.debug("Processing all available data for first time")

        else:
            start_time = await get_and_convert_timestamp(start_time_unix)
            thirty_days_ago = await get_thirty_days_ago()
            correction_start = max(start_time, thirty_days_ago)

            LOGGER.debug(f"Start time: {start_time}")
            LOGGER.debug(f"Start time zone: {start_time.tzinfo}")
            LOGGER.debug(f"Correction start: {correction_start}")
            LOGGER.debug(f"Correction start time zone: {correction_start.tzinfo}")

            # Get the last sum before our correction window to use as base
            stats_before_correction = await get_instance(self.hass).async_add_executor_job(
                statistics_during_period,
                self.hass,
                correction_start,  # From beginning
                None,
                {statistic_id},
                "hour",
                None,
                {"sum"},
            )

            base_sum = (
                stats_before_correction[statistic_id][-1]["sum"]  # Get the last known sum value
                if stats_before_correction and statistic_id in stats_before_correction  # Check if we have prior stats
                else 0.0  # If no prior stats, start from zero
            )

            LOGGER.debug(
                "Stats in correction window: \nFirst: %s\nLast: %s",
                stats_before_correction[statistic_id][0],
                stats_before_correction[statistic_id][-1]
            )

            # Only include data after the correction start time
            usage_data = hourly_df.filter(pl.col(Columns.TIMESTAMP) > pl.lit(correction_start))
            LOGGER.debug(f"Processing data after {correction_start} with base sum {base_sum}")

        if usage_data.height == 0:
            LOGGER.debug("No new data to add to statistics")
            return

        energy_sum = (
            usage_data
            .with_columns(
                (pl.col(Columns.ENERGY_KWH).cum_sum() + base_sum).alias("cum_sum")
            )
        )

        def _create_statistic_data(row: dict[str, Any]) -> StatisticData:
            """Create a StatisticData object from a row of energy data."""
            return StatisticData(
                start=row["timestamp"],
                state=row["energy_kwh"],
                sum=row["cum_sum"],
            )

        # Convert DataFrame rows to statistics
        stats: list[StatisticData] = (
            seq(energy_sum.iter_rows(named=True))
            .map(_create_statistic_data)
            .to_list()
        )

        metadata = StatisticMetaData(
            name=f"Dominion Energy {account_id} Energy Consumption",
            has_mean=False,
            has_sum=True,
            source=DOMAIN,
            statistic_id=statistic_id,
            unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR
        )

        LOGGER.debug(f"Adding {len(stats)} statistics")
        async_add_external_statistics(self.hass, metadata, stats)
