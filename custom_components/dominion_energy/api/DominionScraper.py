import asyncio
import json
from decimal import Decimal
from pathlib import Path
from typing import Union, Optional, Dict
from zoneinfo import ZoneInfo

from arrow import Arrow
from functional import seq
from selenium import webdriver
from selenium.common import WebDriverException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from custom_components.dominion_energy import LOGGER
from custom_components.dominion_energy.exceptions import BrowserException, CannotConnect, InvalidAuth, \
    DominionEnergyException
from custom_components.dominion_energy.models import DominionCredentials, DownloadResult, BillSummary, LoginResult


class DominionScraper:
    def __init__(
            self,
            credentials: DominionCredentials,
            driver_path: str,
            download_directory: Path = Path.cwd(),
    ):
        self.credentials = credentials
        self.download_path = download_directory
        self._driver: Optional[WebDriver] = None
        self._driver_path = driver_path
        self._response_cache: Dict[str, dict] = {}

    def _setup_chrome_options(self) -> Options:
        """Setup Chrome options with network logging enabled."""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable_dev-shm")

        # Enable network logging
        chrome_options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

        # Configure download behavior
        prefs = {
            "download.default_directory": str(self.download_path.absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        return chrome_options

    async def initialize_driver(self) -> None:
        """Initialize the Chrome WebDriver"""
        try:
            if self._driver:
                # Clean up any existing driver
                await asyncio.to_thread(self._driver.quit)

            service = Service(executable_path=self._driver_path)

            self._driver = await asyncio.to_thread(
                webdriver.Chrome,
                service=service,
                options=self._setup_chrome_options(),
            )

            # Enable network tracking
            await asyncio.to_thread(
                self._driver.execute_cdp_cmd,
                'Network.enable',
                {}
            )

        except Exception as exception:
            LOGGER.error("Failed to initialize WebDriver: %s", str(exception))
            if self._driver:
                await self._cleanup_driver()
            raise BrowserException("Failed to initialize browser") from exception

    async def _cleanup_driver(self) -> None:
        """Safely cleanup the WebDriver."""
        if self._driver:
            try:
                # Use a timeout to ensure we don't block forever
                async with asyncio.timeout(15):
                    # Create a separate thread for cleanup
                    await asyncio.to_thread(self._cleanup_driver_sync)
            except asyncio.TimeoutError:
                LOGGER.warning("WebDriver cleanup timed out")
            except Exception as exception:
                LOGGER.error("Error cleaning up WebDriver: %s", str(exception))
            finally:
                await asyncio.to_thread(self._clear_driver)

    def _clear_driver(self) -> None:
        """Clear the driver reference synchronously."""
        if self._driver:
            self._driver = None

    def _cleanup_driver_sync(self) -> None:
        """Synchronous cleanup of WebDriver to be run in separate thread."""
        try:
            if self._driver:
                self._driver.quit()
        except Exception as exception:
            LOGGER.error("Error in sync WebDriver cleanup: %s", str(exception))
        finally:
            # Clear the driver reference here in the sync context
            self._driver = None

    async def _get_network_responses(
            self,
            patterns: list[str] | str,
            params: dict[str, dict[str, str]] | None = None,
            wait_time: int = 5,
    ) -> dict[str, dict]:
        """
        Extract multiple response data from Chrome performance logs.

        Args:
            patterns: List of URL patterns to match or single pattern string
            params: Optional dict of {pattern: {param_key: param_value}} for URL filtering
            wait_time: Time to wait for responses in seconds

        Returns:
            Dictionary mapping pattern keys to response JSON data
        """
        try:
            # Normalize input to list
            url_patterns = [patterns] if isinstance(patterns, str) else patterns
            params = params or {}

            # Wait for responses to be ready
            await asyncio.sleep(wait_time)

            responses: dict[str, dict] = {}
            logs = await asyncio.to_thread(self._driver.get_log, "performance")

            request_ids: dict[str, str] = {}  # pattern -> request_id

            # First pass - collect all matching request IDs
            for entry in logs:
                try:
                    log = json.loads(entry["message"])["message"]
                    if (
                            "Network.responseReceived" not in log["method"]
                            or "response" not in log.get("params", {})
                    ):
                        continue

                    response = log["params"]["response"]
                    url = response.get("url", "")

                    # Skip non-JSON responses
                    if response.get("mimeType") != "application/json":
                        continue

                    # Check URL against all patterns
                    for pattern in url_patterns:
                        if pattern not in url:
                            continue

                        # Check params if specified for this pattern
                        pattern_params = params.get(pattern, {})
                        if pattern_params and not all(
                                f"{key}={value}" in url
                                for key, value in pattern_params.items()
                        ):
                            continue

                        request_ids[pattern] = log["params"]["requestId"]
                        break  # Stop checking patterns once we find a match

                except Exception as entry_exception:
                    LOGGER.warning(
                        "Error processing log entry: %s",
                        str(entry_exception)
                    )
                    continue

            # Second pass - get response bodies
            for pattern, request_id in request_ids.items():
                try:
                    body_response = await asyncio.to_thread(
                        self._driver.execute_cdp_cmd,
                        "Network.getResponseBody",
                        {"requestId": request_id}
                    )
                    responses[pattern] = json.loads(body_response["body"])
                except Exception as response_exception:
                    LOGGER.error(
                        "Failed to get response body for %s: %s",
                        pattern,
                        str(response_exception)
                    )

            return responses

        except Exception as exception:
            LOGGER.error("Failed to get network responses: %s", str(exception))
            return {}

    async def _intercept_network_responses(self):
        """Start intercepting network responses."""
        if not self._driver:
            return

        script = """
            const callback = async (requestId, request) => {
                try {
                    const response = await fetch(request.url);
                    const data = await response.json();
                    window.postMessage({
                        type: 'network-response',
                        url: request.url,
                        response: data
                    }, '*');
                } catch (e) {
                    console.error('Error intercepting response:', e);
                }
            };
            """
        await asyncio.to_thread(self._driver.execute_script, script)

    async def login(self, close_browser_after: bool = False) -> LoginResult:
        """Attempt to log in to Dominion Energy website

        Raises:
        CannotConnect: Error connecting to website
        InvalidAuth: Invalid authentication credentials
        BrowserException: Error with browser automation
        """
        if not self._driver:
            await self.initialize_driver()
            if not self._driver:
                raise BrowserException(f"Failed to initialize browser: No Driver")

        try:
            await asyncio.to_thread(
                self._driver.get,
                "https://login.dominionenergy.com/CommonLogin?SelectedAppName=Electric",
            )

            try:
                email_field = await asyncio.to_thread(
                    WebDriverWait(self._driver, 10).until,
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[id*='gigya-loginID-']")),
                )
            except TimeoutException as exception:
                raise CannotConnect("Could not connect to login page") from exception

            password_field = await asyncio.to_thread(
                self._driver.find_element,
                By.CSS_SELECTOR,
                "[id*='gigya-password-']",
            )

            await asyncio.to_thread(
                self._driver.execute_script,
                "arguments[0].scrollIntoView(true);",
                email_field,
            )
            await asyncio.sleep(1)

            await asyncio.to_thread(email_field.clear)
            await asyncio.to_thread(password_field.clear)

            await asyncio.to_thread(email_field.send_keys, self.credentials.email_address)
            await asyncio.to_thread(password_field.send_keys, self.credentials.password)

            await asyncio.sleep(1)
            submit_buttons = await asyncio.to_thread(
                self._driver.find_elements,
                By.CSS_SELECTOR,
                "input[type='submit'].gigya-input-submit",
            )
            submit_button: WebElement = seq(submit_buttons).last()
            await asyncio.to_thread(submit_button.click)

            try:
                # balance = await asyncio.to_thread(
                #     WebDriverWait(self._driver, 10).until,
                #     self._wait_for_balance,
                # )
                # balance_text = await asyncio.to_thread(lambda: balance.text)
                # LOGGER.debug(f"Current Balance: {balance_text}")  # TODO save balance as entity, also get date due
                return LoginResult(success=True, balance=None)
            except TimeoutException as exception:
                raise InvalidAuth(f"Invalid credentials for {self.credentials.email_address}") from exception

        except InvalidAuth:
            raise
        except TimeoutException as exception:
            raise CannotConnect("Connection timed out") from exception
        except WebDriverException as exception:
            raise BrowserException(f"Browser error: {str(exception)}") from exception
        except Exception as exception:
            raise DominionEnergyException(f"Unexpected error: {str(exception)}") from exception
        finally:
            if self._driver and close_browser_after:
                print("Will close the browser")
                await self._cleanup_driver()

    async def _wait_for_balance(self, driver: webdriver.Chrome) -> Union[WebElement, bool]:
        """Wait for and return the balance element if found."""
        try:
            # Get the element
            element = await asyncio.to_thread(
                driver.find_element,
                By.CSS_SELECTOR,
                "span[class*='currentBalance']"
            )

            # Get the text content
            element_text = await asyncio.to_thread(lambda: element.text.strip())

            # Check if it contains numbers
            is_balance = seq(list(element_text)).exists(lambda char: char.isnumeric())

            return element if is_balance else False
        except WebDriverException:
            return False

    async def _navigate_to_usage(self) -> bool:
        """Navigate to the usage page after log in"""
        if not self._driver:
            return False

        try:
            await asyncio.sleep(10)
            await asyncio.to_thread(
                self._driver.get,
                "https://myaccount.dominionenergy.com/portal/#/Usages",
            )

            # Wait for navigation to complete and page to stabilize
            await asyncio.sleep(10)

            await asyncio.to_thread(
                WebDriverWait(self._driver, 10).until,
                EC.element_to_be_clickable(
                    (By.XPATH, "//a[contains(text(), 'Download 30-minute Data')]")
                ),
            )
            return True
        except (TimeoutException, WebDriverException) as exception:
            LOGGER.error(f"Failed to navigate to usage: {str(exception)}")
            return False

    async def _download_usage_data(self) -> Optional[Path]:
        """Download usage data and return the path to downloaded file."""
        if not self._driver:
            return None

        try:
            download_path = self.download_path
            existing_files = set(download_path.glob("*.xlsx"))

            download_button = await asyncio.to_thread(
                WebDriverWait(self._driver, 10).until,
                EC.element_to_be_clickable(
                    (By.XPATH, "//a[contains(text(), 'Download 30-minute Data')]")
                ),
            )
            await asyncio.sleep(3)
            await asyncio.to_thread(download_button.click)

            timestamp = Arrow.now().format("YYYYMMDD_HHmmss")
            expected_file = download_path / f"dominion_usage_{timestamp}.xlsx"

            def find_new_excel_file():
                new_files = seq(download_path.glob("*.xlsx")).to_set() - existing_files
                if not new_files:
                    return False
                return seq(new_files).first()

            # Wait for download to complete with timeout
            for _ in range(120):  # 2 minute timeout
                new_file = await asyncio.to_thread(find_new_excel_file)
                if new_file:
                    await asyncio.to_thread(new_file.rename, expected_file)
                    return expected_file
                await asyncio.sleep(1)

            return None

        except Exception as exception:
            LOGGER.error(f"Failed to download usage data: {str(exception)}")
            return None

    async def _fetch_bill_summary(self) -> Optional[BillSummary]:
        """Fetch bill summary information from both current and history endpoints."""
        if not self._driver:
            return None

        try:
            await self._intercept_network_responses()

            await asyncio.to_thread(
                self._driver.get,
                "https://myaccount.dominionenergy.com/portal/#/ViewBill",
            )

            await asyncio.sleep(3)

            responses = await self._get_network_responses(
                patterns=["/GetBillandInvoiceHistory", "/current"],
                # Optional URL parameters if needed:
                # params={
                #     "/GetBillandInvoiceHistory": {"param1": "value1"},
                #     "/current": {"param2": "value2"}
                # }
            )

            if not responses:
                LOGGER.error("No responses captured")
                return None

            history_json = responses.get("/GetBillandInvoiceHistory")
            current_json = responses.get("/current")

            if not history_json or not history_json.get('data'):
                LOGGER.error("No history data")
                return None

            if not current_json or not current_json.get('data'):
                LOGGER.error("No current data")
                return None

            current_data = current_json['data'][0]
            extension = current_data.get('extension', {})

            # Get the most recent bill from history
            history_data = history_json['data']['zBillInvHeadtoItemNav']['results'][0]

            bill_summary = BillSummary(
                account_number=current_data['accountNumber'],
                previous_bill_period_start=Arrow.strptime(history_data['billPdStart'], "%m/%d/%Y %H:%M:%S"),
                previous_bill_period_end=Arrow.strptime(history_data['billPdEnd'], "%m/%d/%Y %H:%M:%S"),
                next_meter_read_date=Arrow.strptime(extension['NextMeterReadDate'], "%m-%d-%Y"),
                previous_balance=Decimal(current_data['previousBalance']),
                payments_received=Decimal(current_data['paymentReceived']),
                remaining_balance=Decimal(current_data['remainingBalance']),
                current_charges=Decimal(current_data['currentCharges']),
                total_account_balance=Decimal(current_data['totalAmountDue']),
                pending_payments=Decimal(extension['PendingPaymentAmount'])
                if extension.get('PendingPaymentAmount') else Decimal('0'),
                is_meter_read_estimated=True  # We could potentially detect this from the data
            )
            LOGGER.debug(f"Bill Summary: {bill_summary}")
            return bill_summary

        except Exception as exception:
            LOGGER.error(f"Failed to fetch bill summary: {str(exception)}")
            return None

    async def fetch_usage_data(self, tzinfo: ZoneInfo) -> DownloadResult:
        """Main method to fetch usage data from Dominion Energy website"""
        self.download_path.mkdir(parents=True, exist_ok=True)

        try:
            await self.initialize_driver()

            login_result = await self.login()
            if not login_result.success:
                return DownloadResult(
                    filepath=Path(),
                    timestamp=Arrow.now(),
                    success=False,
                    error="Login failed"
                )

            if not await self._navigate_to_usage():
                return DownloadResult(
                    filepath=Path(),
                    timestamp=Arrow.now(),
                    success=False,
                    error="Navigation to usage failed"
                )

            downloaded_path = await self._download_usage_data()
            if not downloaded_path:
                return DownloadResult(
                    filepath=Path(),
                    timestamp=Arrow.now(),
                    success=False,
                    error="Download of usage data failed",
                )

            bill_summary = await self._fetch_bill_summary()
            bill_summary.update_timezone(tzinfo)

            return DownloadResult(
                filepath=downloaded_path,
                bill_summary=bill_summary,
                timestamp=Arrow.now(),
                success=True,
            )
        except Exception as exception:
            LOGGER.error(f"Unexpected error: {str(exception)}")
            return DownloadResult(
                filepath=Path(),
                timestamp=Arrow.now(),
                success=False,
                error=str(exception)
            )
        finally:
            if self._driver:
                await self._cleanup_driver()

    async def __aenter__(self) -> "DominionScraper":
        """Async context manager entry."""
        await self.initialize_driver()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        if self._driver:
            await self._cleanup_driver()