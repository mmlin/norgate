Requires your own subscription to [Norgate Data](https://norgatedata.com/), US Stocks. I have the Diamond Package,
but this may work with lower tiers as well.

Using Python 3.14.2:

```
pip install -r requirements.txt
python main.py
```

Save the following in Parquet format:

- Historical prices for US stocks.
- Historical prices for US indices.
- Historical US index constituents (via Norgate watchlists)

Took about 17 minutes on my machine and 2.5 GB for the full export.
