from .utils.alphav_functions import alphav_loader

symbols = ["AAPL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "GOOGL", "GOOG"] 

for symbol in symbols:
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol, 
    }

    alphav_loader(
        alphav_params=params,
        source_type="stocks",
        symbol=symbol,
        market="USD",
        interval="daily",
        history_sweep=False
    )