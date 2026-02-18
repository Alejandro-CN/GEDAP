from .utils.alphav_functions import alphav_loader

currencies = ["MXN", "CAD", "EUR", "GBP", "JPY"] 

for currency in currencies:
    params = {
        "function": "FX_DAILY",
        "from_symbol" : "USD",
        "to_symbol" : currency
    }

    alphav_loader(
        alphav_params=params,
        source_type="fx",
        symbol="USD",
        market=currency,
        interval="daily",
        history_sweep=False
    )
