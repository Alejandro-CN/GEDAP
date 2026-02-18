from .utils.alphav_functions import alphav_loader

commodities = [
    {"commodity_id": "WTI", "params": {"function": "WTI", "interval": "daily"}},
    {"commodity_id": "BRE", "params": {"function": "BRENT", "interval": "daily"}},
    {"commodity_id": "NGS", "params": {"function": "NATURAL_GAS", "interval": "daily"}},
    {"commodity_id": "COP", "params": {"function": "COPPER", "interval": "monthly"}},
    {"commodity_id": "CRN", "params": {"function": "CORN", "interval": "monthly"}},
    {"commodity_id": "COF", "params": {"function": "COFFEE", "interval": "monthly"}},
]

for commodity in commodities:
    alphav_loader(
        alphav_params=commodity["params"],
        source_type="commodity",
        symbol=commodity["commodity_id"],   
        market="USD",                   
        interval=commodity["params"]["interval"],
        history_sweep=False
    )
