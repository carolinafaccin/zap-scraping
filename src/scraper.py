import warnings
warnings.filterwarnings("ignore")

import io
import json
import time
import re
import datetime
import math
import random
import subprocess
import urllib.request
import zipfile
import pytz
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    StaleElementReferenceException,
    InvalidSessionIdException,
    WebDriverException,
    TimeoutException,
)

CHROME_PATH = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
_CHROMEDRIVER_CACHE = Path.home() / ".local" / "share" / "chromedriver_arm64" / "chromedriver"

TIPOS = (
    "condominio_residencial,"
    "apartamento_residencial,"
    "studio_residencial,"
    "kitnet_residencial,"
    "casa_residencial,"
    "casa-vila_residencial,"
    "cobertura_residencial,"
    "flat_residencial,"
    "loft_residencial,"
    "lote-terreno_residencial,"
    "sobrado_residencial,"
    "granja_residencial"
)


def _ensure_arm64_chromedriver():
    if _CHROMEDRIVER_CACHE.exists():
        out = subprocess.run(["file", str(_CHROMEDRIVER_CACHE)], capture_output=True, text=True).stdout
        if "arm64" in out:
            return str(_CHROMEDRIVER_CACHE)

    result = subprocess.run([CHROME_PATH, "--version"], capture_output=True, text=True)
    major = result.stdout.strip().split()[-1].split(".")[0]

    print(f"[*] Downloading ARM64 chromedriver for Chrome {major}...")
    api = "https://googlechromelabs.github.io/chrome-for-testing/known-good-versions-with-downloads.json"
    with urllib.request.urlopen(api) as resp:
        data = json.load(resp)

    matches = [v for v in data["versions"] if v["version"].startswith(f"{major}.")]
    if not matches:
        raise RuntimeError(f"No chromedriver found for Chrome {major}")

    arm64_url = next(
        d["url"] for d in matches[-1]["downloads"]["chromedriver"]
        if d["platform"] == "mac-arm64"
    )

    with urllib.request.urlopen(arm64_url) as resp:
        zf = zipfile.ZipFile(io.BytesIO(resp.read()))

    _CHROMEDRIVER_CACHE.parent.mkdir(parents=True, exist_ok=True)
    for name in zf.namelist():
        if name.endswith("/chromedriver"):
            _CHROMEDRIVER_CACHE.write_bytes(zf.read(name))
            _CHROMEDRIVER_CACHE.chmod(0o755)
            break

    # Pre-patch so undetected_chromedriver won't re-patch the binary and invalidate
    # the code signature (which causes macOS to SIGKILL the process).
    binary = _CHROMEDRIVER_CACHE.read_bytes()
    binary = binary.replace(b"cdc_", b"abc_")
    _CHROMEDRIVER_CACHE.write_bytes(binary)

    subprocess.run(["codesign", "--force", "--deep", "--sign", "-", str(_CHROMEDRIVER_CACHE)], check=False)
    subprocess.run(["xattr", "-c", str(_CHROMEDRIVER_CACHE)], check=False)

    print(f"[*] Chromedriver ARM64 ready at {_CHROMEDRIVER_CACHE}")
    return str(_CHROMEDRIVER_CACHE)


class ScraperZap:

    def __init__(self, transacao="aluguel", tipo=TIPOS, local="rs+porto-alegre", precomin=0, precomax=999999999):
        self.base_url = "https://www.zapimoveis.com.br"
        self.transacao = transacao
        self.tipo = tipo
        self.local = local
        self.precomin = precomin
        self.precomax = precomax
        self.timestamp_now = datetime.datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
        self.driver = self._get_driver()
        print(f"[*] Initialized: {transacao} | {local} | R${precomin}-R${precomax}")

    # ── Driver ────────────────────────────────────────────────────────────────

    def _get_driver(self, headless=False):
        options = uc.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument(f"--window-size={random.randint(1200,1920)},{random.randint(800,1080)}")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--start-minimized")
        options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})

        driver = uc.Chrome(
            options=options,
            use_subprocess=True,
            browser_executable_path=CHROME_PATH,
            driver_executable_path=_ensure_arm64_chromedriver(),
        )
        driver.minimize_window()
        time.sleep(random.uniform(1, 3))
        return driver

    def safe_get(self, url, retries=3):
        for attempt in range(retries):
            try:
                self.driver.get(url)
                return
            except WebDriverException as e:
                if "ERR_NAME_NOT_RESOLVED" in str(e):
                    print(f"[!] DNS error (attempt {attempt+1})")
                    time.sleep(random.uniform(2, 5))
                    try:
                        self.safe_quit()
                    except:
                        pass
                    self.driver = self._get_driver()
                else:
                    raise
        raise Exception("Failed to load page after retries")

    def safe_quit(self):
        try:
            if self.driver:
                self.driver.quit()
        except Exception as e:
            print(f"[!] Ignored quit error: {e}")
        finally:
            self.driver = None

    # ── Pagination ────────────────────────────────────────────────────────────

    def _build_url(self, page):
        return (
            f"{self.base_url}/{self.transacao}/apartamentos/{self.local}"
            f"/?pagina={page}&tipos={self.tipo}"
            f"&precoMinimo={self.precomin}&precoMaximo={self.precomax}&ordem=LOWEST_PRICE"
        )

    def get_total_listings(self):
        for _ in range(5):
            try:
                self.safe_get(self._build_url(1))
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "h1"))
                )
                for _ in range(3):
                    try:
                        text = self.driver.find_element(By.TAG_NAME, "h1").text
                        break
                    except StaleElementReferenceException:
                        time.sleep(1)
                else:
                    raise Exception("Failed to read stable h1")

                numbers = re.findall(r"\d+", text.replace(".", "").replace(",", ""))
                total = int(numbers[0]) if numbers else 0
                if total == 0:
                    print("[!] No listings found")
                    return 0
                print(f"[*] Found {total} listings")
                return total

            except InvalidSessionIdException:
                self.safe_quit()
                print("[!] Driver died, restarting in 30s...")
                time.sleep(30)
                self.driver = self._get_driver()

        raise Exception("Failed after retries")

    def _click_next_page(self):
        css_selectors = [
            "[data-cy='rp-pagination-go-to-next-page']",
            "[data-cy='pagination-go-to-next-page']",
            "[data-cy='pagination-next-button']",
            "button[aria-label*='róxima']",
            "a[aria-label*='róxima']",
        ]
        for sel in css_selectors:
            try:
                for el in self.driver.find_elements(By.CSS_SELECTOR, sel):
                    if el.is_displayed() and el.is_enabled():
                        self.driver.execute_script("arguments[0].click();", el)
                        return True
            except:
                continue
        for xpath in ["//button[contains(.,'róxima')]", "//a[contains(.,'róxima')]"]:
            try:
                for el in self.driver.find_elements(By.XPATH, xpath):
                    if el.is_displayed() and el.is_enabled():
                        self.driver.execute_script("arguments[0].click();", el)
                        return True
            except:
                continue
        return False

    # ── Parse ─────────────────────────────────────────────────────────────────

    def parse_page(self, html):
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.find_all(True, {"data-cy": "rp-property-cd"})
        results = []
        for card in cards:
            try:
                link_tag = card.find("a")
                url = link_tag.get("href") if link_tag else None
                id_match = re.search(r"id-(\d+)", url or "")
                imo_id = id_match.group(1) if id_match else None
                if not imo_id:
                    continue

                bairro, cidade = self._extract_bairro_cidade(card)
                preco, periodo_aluguel = self._extract_price_and_period(card, "rp-cardProperty-price-txt")
                condominio, iptu = self._extract_cond_iptu(card)

                results.append({
                    "id": imo_id,
                    "url": url,
                    "transacao": self.transacao,
                    "descricao": self._get_text(card, "h2", "rp-cardProperty-location-txt"),
                    "bairro": bairro,
                    "cidade": cidade,
                    "endereco": self._get_text(card, "p", "rp-cardProperty-street-txt"),
                    "area": self._extract_feature(card, "rp-cardProperty-propertyArea-txt"),
                    "quartos": self._extract_feature(card, "rp-cardProperty-bedroomQuantity-txt"),
                    "banheiros": self._extract_feature(card, "rp-cardProperty-bathroomQuantity-txt"),
                    "garagens": self._extract_feature(card, "rp-cardProperty-parkingSpacesQuantity-txt"),
                    "preco": preco,
                    "periodo_aluguel": periodo_aluguel,
                    "condominio": condominio,
                    "iptu": iptu,
                })
            except Exception as e:
                print(f"[!] Parse error: {e}")
        return results

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _get_text(self, card, tag, data_cy):
        el = card.find(tag, {"data-cy": data_cy})
        return el.text.strip() if el else None

    def _extract_bairro_cidade(self, card):
        try:
            h2 = card.find("h2", {"data-cy": "rp-cardProperty-location-txt"})
            if not h2:
                return None, None
            span = h2.find("span")
            if span:
                span.extract()
            parts = [p.strip() for p in h2.get_text(strip=True).split(",")]
            return parts[0] if parts else None, parts[1] if len(parts) > 1 else None
        except:
            return None, None

    def _extract_price_and_period(self, card, data_cy):
        el = card.find("div", {"data-cy": data_cy})
        if not el:
            return 0.0, None
        text = el.get_text(" ", strip=True).lower()
        match = re.search(r"r\$\s*([\d\.]+)", text)
        price = float(match.group(1).replace(".", "")) if match else 0.0
        period = "mensal" if "mês" in text else "diario" if "dia" in text else "unknown"
        return price, period

    def _extract_cond_iptu(self, card):
        try:
            text = card.get_text(" ", strip=True).lower()
            cond_match = re.search(r"cond\.\s*r\$\s*([\d\.]+)", text)
            iptu_match = re.search(r"iptu\s*r\$\s*([\d\.]+)", text)
            cond = float(cond_match.group(1).replace(".", "")) if cond_match else 0.0
            iptu = float(iptu_match.group(1).replace(".", "")) if iptu_match else 0.0
            return cond, iptu
        except:
            return 0.0, 0.0

    def _extract_feature(self, card, data_cy):
        try:
            el = card.find("li", {"data-cy": data_cy})
            if not el:
                return 0.0
            match = re.search(r"\d+", el.get_text(strip=True))
            return float(match.group()) if match else 0.0
        except:
            return 0.0

    def _p98_price(self, listings):
        prices = sorted(item["preco"] for item in listings if item.get("preco"))
        if not prices:
            return 0
        return prices[max(int(0.98 * len(prices)) - 1, 0)]

    # ── Run ───────────────────────────────────────────────────────────────────

    def _wait_for_listings(self, timeout=30):
        WebDriverWait(self.driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "[data-cy='rp-property-cd']"))
        )

    def _scroll_page(self):
        time.sleep(random.uniform(4, 8))
        for i in range(1, 5):
            self.driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {i/4});")
            time.sleep(0.5)

    def run(self):
        scraped_at = self.timestamp_now.strftime("%Y-%m-%d %H:%M:%S")
        all_data = []
        remaining = 501
        band = 0

        while remaining > 500:
            band += 1
            remaining = self.get_total_listings()
            if remaining == 0:
                break

            pages = min(math.ceil(remaining / 28), 50)
            print(f"[*] Band {band} | R${self.precomin:,} → R${self.precomax:,} | {remaining} listings | {pages} pages")

            # page 1 is already loaded by get_total_listings
            batch = []
            browser_crashed = False
            try:
                self._wait_for_listings(30)
            except TimeoutException:
                print("[!] No listings on page 1 — ending band")
            except InvalidSessionIdException:
                print("[!] Browser crashed on page 1 — restarting")
                browser_crashed = True
            else:
                self._scroll_page()
                page_data = self.parse_page(self.driver.page_source)
                print(f"[*] Page 1/{pages}  — {len(page_data)} listings")
                batch.extend(page_data)

                for page in range(2, pages + 1):
                    time.sleep(random.uniform(8, 15))

                    if not self._click_next_page():
                        print(f"[!] Next page button not found at page {page}")
                        break

                    try:
                        self._wait_for_listings(30)
                    except TimeoutException:
                        print(f"[!] Timeout on page {page} — waiting and retrying...")
                        time.sleep(random.uniform(30, 50))
                        try:
                            self._wait_for_listings(45)
                        except (TimeoutException, InvalidSessionIdException):
                            print(f"[!] No listings on page {page} after retry — ending band early")
                            break
                    except InvalidSessionIdException:
                        print(f"[!] Browser crashed on page {page} — ending band early")
                        browser_crashed = True
                        break

                    self._scroll_page()
                    page_data = self.parse_page(self.driver.page_source)
                    if not page_data:
                        print(f"[!] Empty parse on page {page}")
                        break
                    batch.extend(page_data)
                    print(f"[*] Page {page}/{pages}  — {len(page_data)} listings")

            all_data.extend(batch)

            # Save checkpoint after every band so a crash never loses all data
            if all_data:
                checkpoint = ROOT / "data/scrape" / f"_checkpoint_{self.transacao}.parquet"
                pd.DataFrame(all_data).to_parquet(checkpoint, index=False)
                print(f"[*] Checkpoint saved ({len(all_data)} rows total) → {checkpoint.name}")

            if not batch and band == 1:
                print("[!] First band yielded no data — stopping")
                break

            max_price = self._p98_price(batch) if batch else self.precomin
            if max_price <= self.precomin:
                max_price = int(self.precomin * 1.02) + 1
            self.precomin = int(max_price)

            pages_scraped = len(batch) // 28 + 1
            base_sleep = random.uniform(60, 120)
            extra_sleep = pages_scraped * random.uniform(3, 6)
            if browser_crashed:
                extra_sleep += random.uniform(60, 120)
            sleep_s = int(base_sleep + extra_sleep)
            print(f"[*] Band {band} done ({len(batch)} listings) — restarting browser in {sleep_s}s")
            self.safe_quit()
            time.sleep(sleep_s)
            self.driver = self._get_driver()
            # Visit homepage before next band to warm up the session
            try:
                self.driver.get(self.base_url)
                time.sleep(random.uniform(8, 15))
            except Exception:
                pass

        self.safe_quit()

        if not all_data:
            return pd.DataFrame()

        df = pd.DataFrame(all_data)
        df["scraped_at"] = scraped_at
        return df


ROOT = Path(__file__).resolve().parent.parent


def _load_config():
    import json
    return json.loads((ROOT / "config.json").read_text())


def main(transacao="aluguel"):
    config = _load_config()
    local = f"{config['state']}+{config['city']}"
    label = config.get("label", config["city"])

    (ROOT / "data/scrape").mkdir(parents=True, exist_ok=True)
    start = datetime.datetime.now()
    timestamp = start.strftime("%Y-%m-%d_%H-%M-%S")

    print(f"\n[{start.strftime('%H:%M:%S')}] {'─'*44}")
    print(f"[{start.strftime('%H:%M:%S')}]  {transacao.upper()} | {label}")
    print(f"[{start.strftime('%H:%M:%S')}] {'─'*44}")

    scraper = ScraperZap(transacao=transacao, local=local, tipo=TIPOS)
    df = scraper.run()

    elapsed = (datetime.datetime.now() - start).seconds
    if df.empty:
        print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}]  {transacao}: no data  ({elapsed}s)")
        return None

    df = df.drop_duplicates(subset=["id"], keep="last")
    path = ROOT / "data/scrape" / f"{transacao}_{timestamp}.parquet"
    df.to_parquet(path, index=False)

    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}]  Saved → {path}  ({len(df)} rows, {elapsed}s)")
    return path


if __name__ == "__main__":
    import sys
    transacao = sys.argv[1] if len(sys.argv) >= 2 else "aluguel"
    main(transacao)
