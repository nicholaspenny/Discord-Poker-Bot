import logging

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

logger = logging.getLogger(__name__)


async def scrape_ledger(url: str, timeout: int = 10000) -> pd.DataFrame:
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage']
        )

        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        try:
            await page.goto(url, timeout=timeout, wait_until="networkidle")

            await page.get_by_role("button", name="Log / Ledger").click(timeout=timeout)
            await page.wait_for_timeout(1000)

            await page.get_by_role("button", name="Ledger", exact=True).click(timeout=timeout)
            await page.wait_for_timeout(1000)

            async with page.expect_download() as download_info:
                await page.locator('text="Download Ledger"').click(timeout=timeout)

            download = await download_info.value
            path = await download.path()
            df = pd.read_csv(path, usecols=[0, 1, 7], header=None, skiprows=1, names=["alias", "user_id", "net"])

            def concat_aliases(series):
                seen = set()
                result = []
                for alias in series:
                    if alias not in seen:
                        result.append(alias)
                        seen.add(alias)
                return ",".join(result)

            csv_df = df.groupby("user_id", sort=False).agg({"alias": concat_aliases, "net": "sum"}).reset_index()
            csv_df = csv_df[["alias", "user_id", "net"]]
            return csv_df
        except PlaywrightTimeout as e:
            logger.warning(f"Timeout while downloading ledger from {url}: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.exception(f"Error downloading ledger: {str(e)}")
            return pd.DataFrame()
        finally:
            await browser.close()
