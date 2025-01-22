"""Sensor platform for Dominion Energy historical data."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    UnitOfEnergy,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry
from homeassistant.helpers.device_registry import DeviceInfo, DeviceEntryType
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import StateType
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from polars import DataFrame

from . import DominionEnergyUpdateCoordinator
from .const import DOMAIN
from .models import BillSummary
from .models.attributes import WeeklyAnalysis, DailyStats, BillingPeriodStats


@dataclass(frozen=True, kw_only=True)
class DominionEnergySensorEntityDescription(SensorEntityDescription):
    """Class describing Dominion Energy sensor entities"""
    value_fn: Callable[[DataFrame, BillSummary | None], StateType]
    attributes_fn: Callable[[DataFrame, BillSummary | None], dict[str, Any]] | None = None


ELECTRICITY_SENSOR_DESCRIPTIONS: tuple[DominionEnergySensorEntityDescription, ...] = (
    DominionEnergySensorEntityDescription(
        key="energy_daily",
        name="Latest Day Energy Usage",
        icon="mdi:calendar-today",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL,
        suggested_display_precision=2,
        value_fn=lambda data, _: DailyStats.from_dataframe(data).usage.total_energy_kwh,
        attributes_fn=lambda data, _: DailyStats.from_dataframe(data).to_dict()
    ),
    DominionEnergySensorEntityDescription(
        key="energy_weekly",
        name="Last 7 Days Energy Usage",
        icon="mdi:calendar-week",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL,
        suggested_display_precision=2,
        value_fn=lambda data, _: (
            WeeklyAnalysis.from_dataframe(data).total_energy_kwh
        ),
        attributes_fn=lambda data, _: WeeklyAnalysis.from_dataframe(data).to_dict()
    ),
    DominionEnergySensorEntityDescription(
        key="energy_current_period",
        name="Current Period Usage",
        icon="mdi:lightning-bolt-circle",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL,
        suggested_display_precision=2,
        entity_registry_enabled_default=True,
        suggested_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        value_fn=lambda data, bill: BillingPeriodStats.from_dataframe(data, bill).total_energy_kwh,
        attributes_fn=lambda data, bill: BillingPeriodStats.from_dataframe(data, bill).to_dict()
    ),
)

BILLING_SENSOR_DESCRIPTIONS: tuple[DominionEnergySensorEntityDescription, ...] = (
    DominionEnergySensorEntityDescription(
        key="billing_current_charges",
        name="Current Bill Charges",
        icon="mdi:currency-usd",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement="USD",
        state_class=SensorStateClass.TOTAL,
        suggested_display_precision=2,
        value_fn=lambda data, bill: float(bill.current_charges) if bill else None,
    ),
)

async def async_setup_entry(
        hass: HomeAssistant,
        entry: ConfigEntry,
        async_add_entities: AddEntitiesCallback
) -> None:
    """Set up the Dominion Energy sensors"""
    coordinator: DominionEnergyUpdateCoordinator = hass.data[DOMAIN][entry.entry_id]

    if coordinator.data is not None:
        sensor_descriptions = (
                ELECTRICITY_SENSOR_DESCRIPTIONS +
                BILLING_SENSOR_DESCRIPTIONS
        )
        device = DeviceInfo(
            identifiers={(DOMAIN, entry.entry_id)},
            name=f"Dominion Energy Usage for Account {coordinator.bill_summary.account_number}",
            manufacturer="Dominion Energy",
            model="Usage Data",
            entry_type=DeviceEntryType.SERVICE
        )

        entities = [
            DominionEnergySensor(
                coordinator=coordinator,
                description=description,
                device=device,
                entry_id=entry.entry_id,
                bill_summary=coordinator.bill_summary
            )
            for description in sensor_descriptions
        ]
        async_add_entities(entities)

        # Register the energy sensor with the energy dashboard
        ent_reg = entity_registry.async_get(hass)
        energy_sensor_id = f"{DOMAIN}_{entry.entry_id}_energy_consumption"

        # Get the entity from registry
        if entity_entry := ent_reg.async_get(energy_sensor_id):
            # Add energy dashboard metadata
            ent_reg.async_update_entity_options(
                entity_entry.entity_id,
                "energy",
                {"energy_type": "grid_consumption"},
            )

class DominionEnergySensor(CoordinatorEntity[DominionEnergyUpdateCoordinator], SensorEntity):
    entity_description: DominionEnergySensorEntityDescription

    def __init__(
            self,
            coordinator: DominionEnergyUpdateCoordinator,
            description: DominionEnergySensorEntityDescription,
            device: DeviceInfo,
            entry_id: str,
            bill_summary: BillSummary | None
    ) -> None:
        super().__init__(coordinator)
        self.entity_description = description
        self._attr_unique_id = f"{DOMAIN}_{entry_id}_{description.key}"
        self.entity_id = f"sensor.{DOMAIN}_{description.key}"
        self._attr_device_info = device
        self._bill_summary = bill_summary

    @property
    def native_value(self) -> StateType:
        if self.coordinator.data is not None:
            return  self.entity_description.value_fn(
                self.coordinator.data,
                self._bill_summary
            )

        return None

    @property
    def extra_state_attributes(self) -> Mapping[str, Any] | None:
        if self.coordinator.data is not None and self.entity_description.attributes_fn is not None:
            return self.entity_description.attributes_fn(
                self.coordinator.data,
                self._bill_summary
            )
        return None