"""Config flow for Dominion Energy integration."""
from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
)
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME, CONF_EMAIL
from homeassistant.core import HomeAssistant
from homeassistant.helpers import selector

from .api.DominionScraper import DominionScraper
from .const import DOMAIN, LOGGER
from .exceptions import InvalidAuth, CannotConnect, BrowserException, DominionEnergyException, SetupException
from .models import DominionCredentials
from . import _async_get_or_install_chrome_driver

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_EMAIL): selector.TextSelector(
            selector.TextSelectorConfig(
                type=selector.TextSelectorType.EMAIL,
                autocomplete="email",
            ),
        ),
        vol.Required(CONF_PASSWORD): selector.TextSelector(
            selector.TextSelectorConfig(
                type=selector.TextSelectorType.PASSWORD,
                autocomplete="current-password",
            ),
        ),
    }
)

async def validate_auth(
        hass: HomeAssistant,
        credentials: DominionCredentials
) -> None:
    """Test if credentials are valid."""
    try:
        driver_path = await _async_get_or_install_chrome_driver(hass)

        async with DominionScraper(
                credentials=credentials,
                download_directory=Path(hass.config.path("dominion_energy")),
                driver_path=driver_path
        ) as scraper:
            await scraper.login(close_browser_after=True)
    except InvalidAuth:
        LOGGER.exception("Invalid authentication credentials")
        raise
    except CannotConnect:
        LOGGER.exception("Could not connect to Dominion Energy")
        raise
    except BrowserException:
        LOGGER.exception("Browser automation error")
        raise
    except DominionEnergyException:
        LOGGER.exception("Unexpected error occurred")
        raise


class DominionEnergyConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Dominion Energy."""

    VERSION = 1
    entry: ConfigEntry | None = None

    def __init__(self):
        self.reauth_entry = None

    async def async_step_user(
            self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle the initial step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            # Check for existing instance with same username
            self._async_abort_entries_match({CONF_EMAIL: user_input[CONF_EMAIL]})

            # Test the credentials
            credentials = DominionCredentials(
                email_address=user_input[CONF_EMAIL],
                password=user_input[CONF_PASSWORD],
            )

            try:
                await validate_auth(self.hass, credentials)
            except SetupException:
                errors["base"] = "browser_setup"
            except InvalidAuth:
                errors["base"] = "invalid_auth"
            except CannotConnect:
                errors["base"] = "cannot_connect"
            except BrowserException:
                errors["base"] = "browser_error"
            except Exception:  # pylint: disable=broad-except
                LOGGER.exception("Unexpected exception")
                errors["base"] = "unknown"
            else:
                # Create the config entry
                # noinspection PyTypeChecker
                return self.async_create_entry(
                    title=f"Dominion Energy ({credentials.email_address})",
                    data=user_input,
                )

        # Show the form to the user
        # noinspection PyTypeChecker
        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_DATA_SCHEMA,
            errors=errors,
        )

    async def async_step_reauth(
            self, entry_data: Mapping[str, Any]
    ) -> ConfigFlowResult:
        """Handle re-authentication with Dominion Energy."""
        self.reauth_entry = self.hass.config_entries.async_get_entry(
            self.context["entry_id"]
        )
        # noinspection PyTypeChecker
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
            self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle re-authentication confirmation."""
        errors: dict[str, str] = {}

        if user_input is not None and self.reauth_entry:
            data = {
                **self.reauth_entry.data,
                CONF_PASSWORD: user_input[CONF_PASSWORD],
            }

            credentials = DominionCredentials(
                email_address=self.entry.data[CONF_USERNAME],
                password=user_input[CONF_PASSWORD],
            )

            try:
                await validate_auth(self.hass, credentials)
            except SetupException:
                errors["base"] = "browser_setup"
            except InvalidAuth:
                errors["base"] = "invalid_auth"
            except CannotConnect:
                errors["base"] = "cannot_connect"
            except BrowserException:
                errors["base"] = "browser_error"
            except DominionEnergyException:
                errors["base"] = "dominion_energy"
            except Exception:  # pylint: disable=broad-except
                LOGGER.exception("Unexpected exception")
                errors["base"] = "unknown"
            else:
                self.hass.config_entries.async_update_entry(
                    self.reauth_entry,
                    data=data,
                )
                await self.hass.config_entries.async_reload(self.reauth_entry.entry_id)

                # noinspection PyTypeChecker
                return self.async_abort(reason="reauth_successful")

        # noinspection PyTypeChecker
        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=vol.Schema({
                vol.Required(CONF_PASSWORD): str,
            }),
            errors=errors,
            description_placeholders={
                "username": self.reauth_entry.data[CONF_USERNAME] if self.reauth_entry else "",
            }
        )
