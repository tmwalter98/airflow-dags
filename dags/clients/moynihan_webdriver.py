import asyncio
import logging

from playwright.async_api import Request, Response, async_playwright, expect
from playwright_stealth import Stealth

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

stealth = Stealth()


class MoynihanTrainHallWebDriver:
    def __init__(self):
        self.departure_board: dict[str, str] = {}

    async def handle_response(self, res: Response):
        pass

    async def handle_request(self, req: Request):
        pass

    async def check_boards(self) -> dict[str, str]:
        async with async_playwright() as p:
            # Always launch chromium in non-headless mode or look into advanced arguments
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            page.on("request", self.handle_request)
            page.on("response", self.handle_response)

            # Apply the stealth patches to the page
            await stealth.apply_stealth_async(page)

            await page.goto("https://moynihantrainhall.nyc/transportation/")

            refresh_button = page.locator("#trigger-refresh-amtrak")
            await expect(refresh_button).to_be_enabled(timeout=10000)
            await refresh_button.scroll_into_view_if_needed()
            await refresh_button.click()
            async with page.expect_response("**/admin-ajax.php") as res:
                res_value = await res.value
                self.departure_board = await res_value.json()
            await browser.close()
            return self.departure_board


if __name__ == "__main__":
    driver = MoynihanTrainHallWebDriver()
    asyncio.run(driver.check_boards())
    for k, v in driver.departure_board.items():
        print(k, v)
        print("#" * 15)
