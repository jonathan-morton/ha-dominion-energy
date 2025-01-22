"""Dominion Energy integration."""
from pathlib import Path

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from webdriver_manager.chrome import ChromeDriverManager

from custom_components.dominion_energy.const import DOMAIN, LOGGER
from custom_components.dominion_energy.coordinator import DominionEnergyUpdateCoordinator
from custom_components.dominion_energy.exceptions import SetupException

PLATFORMS: list[Platform] = [Platform.SENSOR]
data_config_path = "dominion_energy"
driver_path_key = "driver_path"
setup_future_key = "setup_future"

async def _async_get_or_install_chrome_driver(hass: HomeAssistant) -> str:
    """Get existing ChromeDriver path or install it."""
    domain_data = hass.data.setdefault(DOMAIN, {})

    # Check if already installed
    if driver_path_key in domain_data:
        return domain_data[driver_path_key]

    if setup_future_key in domain_data:
        try:
            driver_path = await domain_data[setup_future_key]
            return driver_path
        except Exception as exception:
            # remove failed setup
            domain_data.pop(setup_future_key, None)
            raise SetupException(f"ChromeDriver installation failed: {str(exception)}") from exception

    future = hass.loop.create_future()
    domain_data[setup_future_key] = future

    try:
        # Run blocking installation in executor
        driver_path = await hass.async_add_executor_job(
            ChromeDriverManager().install
        )
        domain_data[driver_path_key] = driver_path
        future.set_result(driver_path)
        return driver_path
    except Exception as exception:
        future.set_exception(exception)
        raise SetupException(f"ChromeDriver installation failed: {str(exception)}") from exception
    finally:
        domain_data.pop("setup_future", None)

async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Set up the Dominion Energy integration."""
    try:
        data_directory = Path(hass.config.path(data_config_path))
        data_directory.mkdir(exist_ok=True)

        # Initial installation attempt, will retry during entry setup if it fails
        try:
            await _async_get_or_install_chrome_driver(hass)
        except SetupException as exception:
            LOGGER.warning("Initial ChromeDriver setup failed: %s", str(exception))

        return True

    except Exception as exception:
        LOGGER.error(
            "Failed to setup Dominion Energy integration: %s",
            str(exception)
        )
        return False

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Dominion Energy from a config entry."""
    try:
        try:
            driver_path = await _async_get_or_install_chrome_driver(hass)
        except SetupException as exception:
            LOGGER.error("ChromeDriver setup failed: %s", str(exception))
            return False

        dominion_coordinator = DominionEnergyUpdateCoordinator(
            hass=hass,
            entry_data=entry.data,
            download_dir=Path(hass.config.path(data_config_path)),
            driver_path=driver_path
        )

        await dominion_coordinator.async_config_entry_first_refresh()

        hass.data[DOMAIN][entry.entry_id] = dominion_coordinator
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

        entry.async_on_unload(entry.add_update_listener(async_reload_entry))

        return True
    except Exception as exception:
        LOGGER.error(
            "Failed to setup Dominion Energy integration: %s",
            str(exception)
        )
        raise

async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry"""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)

    if unload_ok:
        # Remove the coordinator
        hass.data[DOMAIN].pop(entry.entry_id)

    return True

async def async_reload_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload the config entry."""
    await async_unload_entry(hass, entry)
    await async_setup_entry(hass, entry)

async def async_remove_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Handle removal of an entry."""
    download_dir = Path(hass.config.path(data_config_path))
    try:
        for file in download_dir.glob("*.xlsx"):
            file.unlink()
        download_dir.rmdir()
    except Exception as exception:
        LOGGER.warning("Error cleaning up files: %s", str(exception))