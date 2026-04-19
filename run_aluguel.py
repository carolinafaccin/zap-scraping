import warnings
warnings.filterwarnings("ignore")

from src.scraper import main as run_scraper
from src.geocode import main as run_geocode
from src.export_gpkg import main as run_export_gpkg


def main():
    scrape_path = run_scraper("aluguel")
    if not scrape_path:
        print("\n[!] Scraping produced no data — stopping.")
        return

    geo_path = run_geocode(str(scrape_path))
    if not geo_path:
        print("\n[!] Geocoding failed — stopping.")
        return

    run_export_gpkg(str(geo_path))


if __name__ == "__main__":
    main()
