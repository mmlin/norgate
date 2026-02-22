"""Exports data from Norgate."""

import logging
from pathlib import Path
import time

import norgatedata as nd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

_INDEX_NAMES = [
    "Dow Jones Industrial Average",
    "Nasdaq 100",
    "Nasdaq Biotechnology",
    "Nasdaq Composite",
    "Nasdaq Internet",
    "Nasdaq Next Generation 100",
    "Nasdaq Q-50",
    "NYSE Composite",
    "Russell 1000",
    "Russell 2000",
    "Russell 3000",
    "Russell Micro Cap",
    "Russell Mid Cap",
    "Russell Small Cap Completeness",
    "Russell Top 200",
    "S&P 100",
    "S&P 1000",
    "S&P 500 Dividend Aristocrats",
    "S&P 500 ESG",
    "S&P 500 excl S&P 100",
    "S&P 500",
    "S&P 900",
    "S&P Composite 1500",
    "S&P MidCap 400",
    "S&P SmallCap 600",
]

_WATCHLIST_INDICES = {
    "Dow Jones Industrial Average Current & Past": ["Dow Jones Industrial Average"],
    "Nasdaq 100 + Next Generation 100 Superset Current & Past": [
        "Nasdaq 100",
        "Nasdaq Next Generation 100",
    ],
    "Nasdaq 100 + Q-50 Superset Current & Past": ["Nasdaq 100", "Nasdaq Q-50"],
    "Nasdaq 100 Current & Past": ["Nasdaq 100"],
    "Nasdaq 100 Technology Sector Current & Past": ["Nasdaq 100 Technology Sector"],
    "Nasdaq Biotechnology Current & Past": ["Nasdaq Biotechnology"],
    "Nasdaq Composite Current & Past": ["Nasdaq Composite"],
    "Nasdaq Internet Current & Past": ["Nasdaq Internet"],
    "Nasdaq Next Generation 100 Current & Past": ["Nasdaq Next Generation 100"],
    "Nasdaq Q-50 Current & Past": ["Nasdaq Q-50"],
    "NYSE Composite Current & Past": ["NYSE Composite"],
    "Russell 1000 + 2000 + Micro Cap Superset Current & Past": [
        "Russell 1000",
        "Russell 2000",
        "Russell Micro Cap",
    ],
    "Russell 1000 Current & Past": ["Russell 1000"],
    "Russell 2000 + Micro Cap Superset Current & Past": [
        "Russell 2000",
        "Russell Micro Cap",
    ],
    "Russell 2000 bottom 1000 Current & Past": ["Russell 2000 bottom 1000"],
    "Russell 2000 Current & Past": ["Russell 2000"],
    "Russell 3000 Current & Past": ["Russell 3000"],
    "Russell Micro Cap Current & Past": ["Russell Micro Cap"],
    "Russell Micro Cap excl Russell 2000 Current & Past": [
        "Russell Micro Cap excl Russell 2000"
    ],
    "Russell Mid Cap Current & Past": ["Russell Mid Cap"],
    "Russell Small Cap Completeness Current & Past": ["Russell Small Cap Completeness"],
    "Russell Top 200 Current & Past": ["Russell Top 200"],
    "S&P 100 Current & Past": ["S&P 100"],
    "S&P 1000 Current & Past": ["S&P 1000"],
    "S&P 500 Current & Past": ["S&P 500"],
    "S&P 500 Dividend Aristocrats Current & Past": ["S&P 500 Dividend Aristocrats"],
    "S&P 500 ESG Current & Past": ["S&P 500 ESG"],
    "S&P 500 excl S&P 100 Current & Past": ["S&P 500 excl S&P 100"],
    "S&P 900 Current & Past": ["S&P 900"],
    "S&P Composite 1500 Current & Past": ["S&P Composite 1500"],
    "S&P MidCap 400 Current & Past": ["S&P MidCap 400"],
    "S&P SmallCap 600 Current & Past": ["S&P SmallCap 600"],
}


def _export_assets():
    """Exports full price history for assets in all available watchlists.

    Saves to data/assets/asset_symbol.parquet. Also saves metadata to data/metadata/assets.parquet.
    """
    symbols = set()
    for watchlist in nd.watchlists():
        watchlist_symbols = nd.watchlist_symbols(watchlist)
        symbols.update(watchlist_symbols)
    logging.info(f"Exporting {len(symbols)} symbols...")

    base_path = Path("data")
    assets_path = base_path / "assets"
    assets_path.mkdir(parents=True, exist_ok=True)
    metadata = []
    exported = 0
    for symbol in symbols:
        asset_id = nd.assetid(symbol)
        metadata.append(
            {
                "asset_id": asset_id,
                "symbol": symbol,
                "exchange_name": nd.exchange_name(symbol),
                "classification": nd.classification(symbol, "GICS", "Name"),
            }
        )

        data = nd.price_timeseries(symbol)
        table = pa.Table.from_pydict({name: data[name] for name in data.dtype.names})
        pq.write_table(table, assets_path / f"{symbol}.parquet")

        exported += 1
        if exported % 1000 == 0:
            logging.info(f"{exported} exported.")

    metadata_path = base_path / "metadata"
    metadata_path.mkdir(exist_ok=True)
    pd.DataFrame(metadata).to_csv(metadata_path / "assets.csv")


def _export_indices():
    """Exports full price history for all available US indices.

    Saves to data/indices/index_name.parquet.
    """
    symbols = nd.database_symbols("US Indices")
    logging.info(f"Exporting {len(symbols)} indices...")

    base_path = Path("data")
    indices_path = base_path / "indices"
    indices_path.mkdir(parents=True, exist_ok=True)
    exported = 0
    for symbol in symbols:
        data = nd.price_timeseries(symbol)
        table = pa.Table.from_pydict({name: data[name] for name in data.dtype.names})
        pq.write_table(table, indices_path / f"{symbol}.parquet")

        exported += 1
        if exported % 1000 == 0:
            logging.info(f"{exported} exported.")


def _export_watchlists():
    """Exports index constituents for all available watchlists.

    Each watchlist is saved to data/watchlists/watchlist_name.parquet. Each row is a date and column an asset. Cell
    values show whether that asset belonged to that watchlist on that date, meaning it was a constituent of one of its
    indices.
    """
    logging.info(f"Exporting {len(_WATCHLIST_INDICES)} watchlists...")

    watchlists_path = Path("data") / "watchlists"
    watchlists_path.mkdir(parents=True, exist_ok=True)
    for watchlist, indices in _WATCHLIST_INDICES.items():
        symbol_dfs = []
        for symbol in nd.watchlist_symbols(watchlist):
            symbol_df = None
            for index in indices:
                df = nd.index_constituent_timeseries(
                    symbol, index, timeseriesformat="pandas-dataframe"
                )
                if symbol_df is None:
                    symbol_df = df
                else:
                    symbol_df |= df

            symbol_df.columns = [symbol]
            symbol_dfs.append(symbol_df)

        # Save the watchlist data.
        watchlist_df = pd.concat(symbol_dfs, axis=1, sort=True)
        watchlist_df.fillna(0, inplace=True)
        watchlist_df.to_parquet(watchlists_path / f"{watchlist}.parquet")
        logging.info(f"Exported {watchlist}")


if __name__ == "__main__":
    log_dir = Path("data")
    log_dir.mkdir(exist_ok=True)
    logging.basicConfig(
        filename=log_dir / "info.txt",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filemode="w",
    )

    start = split_start = time.perf_counter()
    _export_assets()
    split_end = time.perf_counter()
    logging.info(f"Elapsed time: {split_end - split_start:.3f} seconds")

    split_start = time.perf_counter()
    _export_indices()
    split_end = time.perf_counter()
    logging.info(
        f"Elapsed time: {split_end - split_start:.3f} seconds ({split_end - start:.3f}s total)"
    )

    split_start = time.perf_counter()
    _export_watchlists()
    split_end = time.perf_counter()
    logging.info(
        f"Elapsed time: {split_end - split_start:.3f} seconds ({split_end - start:.3f}s total)"
    )
