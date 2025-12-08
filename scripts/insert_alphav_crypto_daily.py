from .utils.alphav_functions import alphav_loader

cryptos = ["BTC", "ETH", "USDT", "USDC", "SOL"] 

for crypto in cryptos:
    params = {
        "function": "DIGITAL_CURRENCY_DAILY",
        "symbol" : crypto,
        "market" : "USD"
    }

    alphav_loader(
        alphav_params=params,
        source_type="crypto",
        symbol=crypto,
        market="USD",
        interval="daily"
    )