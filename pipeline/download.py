import requests
from pathlib import Path


TOUR_REPOS = {
    "atp": "JeffSackmann/tennis_atp",
    "wta": "JeffSackmann/tennis_wta",
}


def build_url(tour: str, year: int, data_type: str = "matches") -> str:
    """Build the raw GitHub URL for a given tour, year and data type."""
    repo = TOUR_REPOS[tour.lower()]
    filename = f"{tour.lower()}_{data_type}_{year}.csv"
    return f"https://raw.githubusercontent.com/{repo}/master/{filename}"


def download_csv(
    tour: str,
    year: int,
    dest_dir: str,
    data_type: str = "matches",
) -> Path:
    """
    Download a single tennis CSV file from GitHub.

    Args:
        tour: "atp" or "wta"
        year: year to download (1968-2024)
        dest_dir: local directory to save the file
        data_type: type of data to download (default: "matches")

    Returns:
        Path to the downloaded file
    """
    if tour.lower() not in TOUR_REPOS:
        raise ValueError(f"Invalid tour '{tour}'. Must be 'atp' or 'wta'.")
    if not 1968 <= year <= 2024:
        raise ValueError(f"Year {year} out of range. Must be between 1968 and 2024.")

    url = build_url(tour, year, data_type)
    dest_path = Path(dest_dir) / f"{tour.lower()}_{data_type}_{year}.csv"
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    dest_path.write_bytes(response.content)
    print(f"Downloaded {url} -> {dest_path}")
    return dest_path


def download_range(
    tour: str,
    start_year: int,
    end_year: int,
    dest_dir: str,
    data_type: str = "matches",
) -> list[Path]:
    """Download CSVs for a range of years."""
    return [
        download_csv(tour, year, dest_dir, data_type)
        for year in range(start_year, end_year + 1)
    ]