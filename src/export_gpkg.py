import sys
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

ROOT = Path(__file__).resolve().parent.parent


def main(parquet_path):
    src = Path(parquet_path)
    if not src.exists():
        print(f"[!] File not found: {src}")
        return

    df = pd.read_parquet(src)

    before = len(df)
    df = df[df["latitude"].notna() & df["longitude"].notna()].copy()
    dropped = before - len(df)

    if df.empty:
        print("[!] No rows with coordinates — run geocode.py first")
        return

    df["geometry"] = df.apply(lambda r: Point(r["longitude"], r["latitude"]), axis=1)
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    gdf = gdf.drop(columns=["latitude", "longitude"])

    out_dir = ROOT / "data/gpkg"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / src.with_suffix(".gpkg").name
    gdf.to_file(out_path, driver="GPKG")

    print(f"  Saved  {out_path}  ({len(gdf)} features, {dropped} rows skipped — no coordinates)")
    return out_path


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        target = sys.argv[1]
    else:
        parquets = sorted((ROOT / "data/geocode").glob("*.parquet"))
        if not parquets:
            print("[!] No parquet files found in data/geocode/")
            sys.exit(1)
        target = parquets[-1]
        print(f"[*] Using most recent: {target}")

    main(str(target))
