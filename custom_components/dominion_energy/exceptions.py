"""Exceptions for Dominion Energy integration."""

class DominionEnergyException(Exception):
    """Base exception for Dominion Energy integration."""


class CannotConnect(DominionEnergyException):
    """Error to indicate we cannot connect."""


class InvalidAuth(DominionEnergyException):
    """Error to indicate there is invalid auth."""


class BrowserException(DominionEnergyException):
    """Error to indicate there was a browser error."""

class SetupException(DominionEnergyException):
    """Error during setup."""