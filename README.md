# REALM
**REALM** - *Real Estate Data Location Mining* - is a web scraping project designed to extract and analyze real estate data from leading Brazilian platform.

## Goals

- **Data collection**: Develop a flexible web scraper to extract real estate listings, including key attributes like price, location, size, number of rooms, and building features.

- **Data structuring**: Organize the scraped data into a clean, structured format (e.g., CSV and Parquet) suitable for analysis.

- **Georeferrencing**: Geocode the data, adding latitude and longitude for each entry.

- **Initial analysis**: Conduct a preliminary analysis calculating price per square meter, per H3 Hexagonal grid (resolution 9).

## Running the Project

Clone the repository and run the main script: `python run.py`

## Repository Structure

```
ZAP/
├── data/             # Data files (add to .gitignore)
│   ├── aggregate/
│   ├── geocode/
│   └── scrape/
├── src/              # Source code
├── run.py            # Main script
├── .gitignore
└── README.md
```