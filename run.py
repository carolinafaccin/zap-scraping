import warnings
warnings.filterwarnings("ignore")

from src.scraper import main as run_scraper
from src.geocode import main as run_geocode
from src.export_gpkg import main as run_export_gpkg


def main():
    for transacao in ["venda", "aluguel"]:
        scrape_path = run_scraper(transacao)
        if not scrape_path:
            print(f"\n[!] Scraping {transacao} produced no data — skipping.")
            continue

        geo_path = run_geocode(str(scrape_path))
        if not geo_path:
            print(f"\n[!] Geocoding {transacao} failed — skipping.")
            continue

        run_export_gpkg(str(geo_path))


if __name__ == "__main__":
    main()
