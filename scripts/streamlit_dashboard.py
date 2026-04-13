import streamlit as st
import pandas as pd
import sqlite3
import plotly.graph_objects as go
from pathlib import Path
from datetime import date

# Database path
DB_PATH = Path(__file__).parent.parent / "GEDAP_DB.db"

st.set_page_config(layout="wide", page_title="Financial Data Dashboard")
st.title("Financial Data Dashboard")

# Connect to database (create fresh connection each time to avoid threading issues)
def get_db_connection():
    return sqlite3.connect(DB_PATH)

# ========================= STOCKS FUNCTIONS =========================
@st.cache_data
def get_unique_symbols():
    conn = get_db_connection()
    query = "SELECT DISTINCT symbol FROM alphav_stocks_daily ORDER BY symbol"
    symbols = pd.read_sql_query(query, conn)["symbol"].tolist()
    conn.close()
    return sorted(symbols)

@st.cache_data
def get_stock_data(symbols):
    conn = get_db_connection()
    placeholders = ",".join("?" * len(symbols))
    query = f"SELECT * FROM alphav_stocks_daily WHERE symbol IN ({placeholders}) ORDER BY date"
    df = pd.read_sql_query(query, conn, params=symbols)
    df["date"] = pd.to_datetime(df["date"])
    conn.close()
    return df

@st.cache_data
def get_stock_date_range():
    conn = get_db_connection()
    query = "SELECT MIN(date) as min_date, MAX(date) as max_date FROM alphav_stocks_daily"
    result = pd.read_sql_query(query, conn)
    conn.close()
    return pd.to_datetime(result["min_date"].iloc[0]).date(), pd.to_datetime(result["max_date"].iloc[0]).date()

# ========================= FX FUNCTIONS =========================
@st.cache_data
def get_unique_currencies():
    conn = get_db_connection()
    query = """SELECT DISTINCT from_currency FROM alphav_fx_daily UNION SELECT DISTINCT to_currency FROM alphav_fx_daily ORDER BY 1"""
    currencies = pd.read_sql_query(query, conn)["from_currency"].tolist()
    conn.close()
    return sorted(currencies)

@st.cache_data
def get_fx_data(from_currencies, to_currencies):
    conn = get_db_connection()
    placeholders_from = ",".join("?" * len(from_currencies))
    placeholders_to = ",".join("?" * len(to_currencies))
    query = f"SELECT * FROM alphav_fx_daily WHERE from_currency IN ({placeholders_from}) AND to_currency IN ({placeholders_to}) ORDER BY date"
    params = from_currencies + to_currencies
    df = pd.read_sql_query(query, conn, params=params)
    df["date"] = pd.to_datetime(df["date"])
    conn.close()
    return df

@st.cache_data
def get_fx_date_range():
    conn = get_db_connection()
    query = "SELECT MIN(date) as min_date, MAX(date) as max_date FROM alphav_fx_daily"
    result = pd.read_sql_query(query, conn)
    conn.close()
    return pd.to_datetime(result["min_date"].iloc[0]).date(), pd.to_datetime(result["max_date"].iloc[0]).date()

# ========================= CRYPTO FUNCTIONS =========================
@st.cache_data
def get_unique_cryptocurrencies():
    conn = get_db_connection()
    query = "SELECT DISTINCT crypto_code FROM alphav_crypto_daily ORDER BY crypto_code"
    cryptos = pd.read_sql_query(query, conn)["crypto_code"].tolist()
    conn.close()
    return sorted(cryptos)

@st.cache_data
def get_crypto_data(cryptocurrencies):
    conn = get_db_connection()
    placeholders = ",".join("?" * len(cryptocurrencies))
    query = f"SELECT * FROM alphav_crypto_daily WHERE crypto_code IN ({placeholders}) AND fiat_currency = 'USD' ORDER BY date"
    df = pd.read_sql_query(query, conn, params=cryptocurrencies)
    df["date"] = pd.to_datetime(df["date"])
    conn.close()
    return df

@st.cache_data
def get_crypto_date_range():
    conn = get_db_connection()
    query = "SELECT MIN(date) as min_date, MAX(date) as max_date FROM alphav_crypto_daily WHERE fiat_currency = 'USD'"
    result = pd.read_sql_query(query, conn)
    conn.close()
    return pd.to_datetime(result["min_date"].iloc[0]).date(), pd.to_datetime(result["max_date"].iloc[0]).date()

# ========================= COMMODITY FUNCTIONS =========================
@st.cache_data
def get_unique_commodities():
    conn = get_db_connection()
    query = "SELECT DISTINCT commodity_id FROM alphav_commodity ORDER BY commodity_id"
    commodities = pd.read_sql_query(query, conn)["commodity_id"].tolist()
    conn.close()
    return sorted(commodities)

@st.cache_data
def get_commodity_lookup():
    conn = get_db_connection()
    query = "SELECT commodity_id, commodity_name, interval, unit FROM alphav_commodity_lookup"
    lookup = pd.read_sql_query(query, conn)
    conn.close()
    return lookup

@st.cache_data
def get_commodity_data(commodities):
    conn = get_db_connection()
    placeholders = ",".join("?" * len(commodities))
    query = f"SELECT * FROM alphav_commodity WHERE commodity_id IN ({placeholders}) ORDER BY date"
    df = pd.read_sql_query(query, conn, params=commodities)
    df["date"] = pd.to_datetime(df["date"])
    conn.close()
    return df

@st.cache_data
def get_commodity_date_range():
    conn = get_db_connection()
    query = "SELECT MIN(date) as min_date, MAX(date) as max_date FROM alphav_commodity"
    result = pd.read_sql_query(query, conn)
    conn.close()
    return pd.to_datetime(result["min_date"].iloc[0]).date(), pd.to_datetime(result["max_date"].iloc[0]).date()

# ========================= CREATE TABS =========================
tab1, tab2, tab3, tab4 = st.tabs(["📈 Stocks", "💱 FX", "₿ Crypto", "🛢️ Commodity"])

# ========================= STOCKS TAB =========================
with tab1:
    st.header("Stock Daily Data Viewer")
    
    col1, col2, col3, col4 = st.columns(4)
    
    symbols = get_unique_symbols()
    stock_min_date, stock_max_date = get_stock_date_range()
    default_stock_start = date(2026, 1, 1) if date(2026, 1, 1) >= stock_min_date else stock_min_date
    
    with col1:
        selected_symbols = st.multiselect("Select stock symbol(s)", options=symbols, default=symbols[0] if symbols else None, key="stock_symbols")
    
    with col2:
        ohlc_options_stock = st.multiselect("Select price data", options=["open", "high", "low", "close"], default=["open", "high", "low", "close"], key="stock_ohlc")
    
    with col3:
        stock_min_date_filter = st.date_input("Start date", value=default_stock_start, min_value=stock_min_date, max_value=stock_max_date, key="stock_min_date")
    
    with col4:
        stock_max_date_filter = st.date_input("End date", value=stock_max_date, min_value=stock_min_date, max_value=stock_max_date, key="stock_max_date")
    
    if selected_symbols and ohlc_options_stock:
        df_stock = get_stock_data(selected_symbols)
        if not df_stock.empty:
            df_stock_filtered = df_stock[(df_stock["date"].dt.date >= stock_min_date_filter) & (df_stock["date"].dt.date <= stock_max_date_filter)]
            if not df_stock_filtered.empty:
                st.subheader(f"Data for {', '.join(selected_symbols)}")
                
                fig_stock_prices = go.Figure()
                unique_symbols = df_stock_filtered.groupby("symbol").size().index.tolist()
                
                for symbol in unique_symbols:
                    df_symbol = df_stock_filtered[df_stock_filtered["symbol"] == symbol]
                    for col in ohlc_options_stock:
                        fig_stock_prices.add_trace(go.Scatter(
                            x=df_symbol["date"], y=df_symbol[col], mode="lines",
                            name=f"{symbol} {col.capitalize()}", legendgroup=symbol,
                            hovertemplate=f"<b>{symbol}</b><br>Date: %{{x|%Y-%m-%d}}<br>{col.capitalize()}: %{{y:.2f}}<extra></extra>"
                        ))
                
                fig_stock_prices.update_layout(title="", xaxis_title="Date", yaxis_title="Price (USD)", hovermode="x unified", height=500, template="plotly_white", legend=dict(orientation="v", yanchor="top", y=0.99, xanchor="left", x=1.01))
                st.plotly_chart(fig_stock_prices, use_container_width=True)
                
                fig_stock_volume = go.Figure()
                for symbol in unique_symbols:
                    df_symbol = df_stock_filtered[df_stock_filtered["symbol"] == symbol]
                    fig_stock_volume.add_trace(go.Bar(x=df_symbol["date"], y=df_symbol["volume"], name=f"{symbol} Volume", legendgroup=symbol, hovertemplate=f"<b>{symbol}</b><br>Date: %{{x|%Y-%m-%d}}<br>Volume: %{{y:,.0f}}<extra></extra>"))
                
                fig_stock_volume.update_layout(title="", xaxis_title="Date", yaxis_title="Volume", hovermode="x unified", height=300, template="plotly_white", legend=dict(orientation="v", yanchor="top", y=0.99, xanchor="left", x=1.01))
                st.plotly_chart(fig_stock_volume, use_container_width=True)
                
                with st.expander("View Raw Data"):
                    st.dataframe(df_stock_filtered, use_container_width=True)
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Latest Close", f"${df_stock_filtered['close'].iloc[-1]:.2f}")
                with col2:
                    st.metric("Highest", f"${df_stock_filtered['high'].max():.2f}")
                with col3:
                    st.metric("Lowest", f"${df_stock_filtered['low'].min():.2f}")
                with col4:
                    st.metric("Records", len(df_stock_filtered))
            else:
                st.warning("No data found for the selected date range")
        else:
            st.warning("No data found for the selected symbols")

# ========================= FX TAB =========================
with tab2:
    st.header("Foreign Exchange Daily Data Viewer")
    
    col1, col2, col3, col4 = st.columns(4)
    
    currencies = get_unique_currencies()
    fx_min_date, fx_max_date = get_fx_date_range()
    default_fx_start = date(2026, 1, 1) if date(2026, 1, 1) >= fx_min_date else fx_min_date
    
    usd_only = [c for c in currencies if c == "USD"]
    
    with col1:
        from_currencies_fx = st.multiselect("Select FROM currency", options=usd_only, default=usd_only, key="fx_from_currency", disabled=True)
        st.caption("✓ Only USD available")
    
    with col2:
        ohlc_options_fx = st.multiselect("Select price data", options=["open", "high", "low", "close"], default=["open", "high", "low", "close"], key="fx_ohlc")
    
    with col3:
        fx_min_date_filter = st.date_input("Start date", value=default_fx_start, min_value=fx_min_date, max_value=fx_max_date, key="fx_min_date")
    
    with col4:
        fx_max_date_filter = st.date_input("End date", value=fx_max_date, min_value=fx_min_date, max_value=fx_max_date, key="fx_max_date")
    
    col1_to, col2_to, col3_to, col4_to = st.columns(4)
    with col1_to:
        to_currencies_fx = st.multiselect("Select TO currency", options=currencies, default=currencies[1] if len(currencies) > 1 else (currencies[0] if currencies else None), key="fx_to_currency")
    
    if from_currencies_fx and to_currencies_fx and ohlc_options_fx:
        df_fx = get_fx_data(from_currencies_fx, to_currencies_fx)
        if not df_fx.empty:
            df_fx_filtered = df_fx[(df_fx["date"].dt.date >= fx_min_date_filter) & (df_fx["date"].dt.date <= fx_max_date_filter)]
            if not df_fx_filtered.empty:
                st.subheader(f"Data for {', '.join(from_currencies_fx)} → {', '.join(to_currencies_fx)}")
                
                fig_fx = go.Figure()
                currency_pairs = df_fx_filtered.groupby("to_currency").size().index.tolist()
                
                for to_curr in currency_pairs:
                    df_curr = df_fx_filtered[df_fx_filtered["to_currency"] == to_curr]
                    for col in ohlc_options_fx:
                        fig_fx.add_trace(go.Scatter(
                            x=df_curr["date"], y=df_curr[col], mode="lines",
                            name=f"USD/{to_curr} {col.capitalize()}", legendgroup=to_curr,
                            hovertemplate=f"<b>USD/{to_curr}</b><br>Date: %{{x|%Y-%m-%d}}<br>{col.capitalize()}: %{{y:.4f}}<extra></extra>"
                        ))
                
                fig_fx.update_layout(title="", xaxis_title="Date", yaxis_title="Price (USD)", hovermode="x unified", height=500, template="plotly_white", legend=dict(orientation="v", yanchor="top", y=0.99, xanchor="left", x=1.01))
                st.plotly_chart(fig_fx, use_container_width=True)
                
                with st.expander("View Raw Data"):
                    st.dataframe(df_fx_filtered, use_container_width=True)
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Latest Close", f"{df_fx_filtered['close'].iloc[-1]:.4f}")
                with col2:
                    st.metric("Highest", f"{df_fx_filtered['high'].max():.4f}")
                with col3:
                    st.metric("Lowest", f"{df_fx_filtered['low'].min():.4f}")
                with col4:
                    st.metric("Records", len(df_fx_filtered))
            else:
                st.warning("No data found for the selected date range")
        else:
            st.warning("No data found for the selected currencies")

# ========================= CRYPTO TAB =========================
with tab3:
    st.header("Cryptocurrency Daily Data Viewer")
    
    col1, col2, col3, col4 = st.columns(4)
    
    cryptocurrencies = get_unique_cryptocurrencies()
    crypto_min_date, crypto_max_date = get_crypto_date_range()
    default_crypto_start = date(2026, 1, 1) if date(2026, 1, 1) >= crypto_min_date else crypto_min_date
    
    with col1:
        selected_cryptos = st.multiselect("Select cryptocurrency", options=cryptocurrencies, default=cryptocurrencies[0] if cryptocurrencies else None, key="crypto_list")
    
    with col2:
        ohlc_options_crypto = st.multiselect("Select price data", options=["open", "high", "low", "close"], default=["open", "high", "low", "close"], key="crypto_ohlc")
    
    with col3:
        crypto_min_date_filter = st.date_input("Start date", value=default_crypto_start, min_value=crypto_min_date, max_value=crypto_max_date, key="crypto_min_date")
    
    with col4:
        crypto_max_date_filter = st.date_input("End date", value=crypto_max_date, min_value=crypto_min_date, max_value=crypto_max_date, key="crypto_max_date")
    
    if selected_cryptos and ohlc_options_crypto:
        df_crypto = get_crypto_data(selected_cryptos)
        if not df_crypto.empty:
            df_crypto_filtered = df_crypto[(df_crypto["date"].dt.date >= crypto_min_date_filter) & (df_crypto["date"].dt.date <= crypto_max_date_filter)]
            if not df_crypto_filtered.empty:
                st.subheader(f"Data for {', '.join(selected_cryptos)} → USD")
                
                fig_crypto_prices = go.Figure()
                unique_cryptos = df_crypto_filtered.groupby("crypto_code").size().index.tolist()
                
                for crypto in unique_cryptos:
                    df_crypto_coin = df_crypto_filtered[df_crypto_filtered["crypto_code"] == crypto]
                    for col in ohlc_options_crypto:
                        fig_crypto_prices.add_trace(go.Scatter(
                            x=df_crypto_coin["date"], y=df_crypto_coin[col], mode="lines",
                            name=f"{crypto}/USD {col.capitalize()}", legendgroup=crypto,
                            hovertemplate=f"<b>{crypto}/USD</b><br>Date: %{{x|%Y-%m-%d}}<br>{col.capitalize()}: $%{{y:.2f}}<extra></extra>"
                        ))
                
                fig_crypto_prices.update_layout(title="", xaxis_title="Date", yaxis_title="Price (USD)", hovermode="x unified", height=500, template="plotly_white", legend=dict(orientation="v", yanchor="top", y=0.99, xanchor="left", x=1.01))
                st.plotly_chart(fig_crypto_prices, use_container_width=True)
                
                fig_crypto_volume = go.Figure()
                for crypto in unique_cryptos:
                    df_crypto_coin = df_crypto_filtered[df_crypto_filtered["crypto_code"] == crypto]
                    fig_crypto_volume.add_trace(go.Bar(x=df_crypto_coin["date"], y=df_crypto_coin["volume"], name=f"{crypto} Volume", legendgroup=crypto, hovertemplate=f"<b>{crypto}</b><br>Date: %{{x|%Y-%m-%d}}<br>Volume: %{{y:,.0f}}<extra></extra>"))
                
                fig_crypto_volume.update_layout(title="", xaxis_title="Date", yaxis_title="Volume", hovermode="x unified", height=300, template="plotly_white", legend=dict(orientation="v", yanchor="top", y=0.99, xanchor="left", x=1.01))
                st.plotly_chart(fig_crypto_volume, use_container_width=True)
                
                with st.expander("View Raw Data"):
                    st.dataframe(df_crypto_filtered, use_container_width=True)
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Latest Close", f"${df_crypto_filtered['close'].iloc[-1]:.2f}")
                with col2:
                    st.metric("Highest", f"${df_crypto_filtered['high'].max():.2f}")
                with col3:
                    st.metric("Lowest", f"${df_crypto_filtered['low'].min():.2f}")
                with col4:
                    st.metric("Records", len(df_crypto_filtered))
            else:
                st.warning("No data found for the selected date range")
        else:
            st.warning("No data found for the selected cryptocurrencies")

# ========================= COMMODITY TAB =========================
with tab4:
    st.header("Commodity Data Viewer")
    
    col1, col2, col3, col4 = st.columns(4)
    
    commodities = get_unique_commodities()
    commodity_lookup = get_commodity_lookup()
    commodity_name_map = dict(zip(commodity_lookup["commodity_id"], commodity_lookup["commodity_name"]))
    comm_min_date, comm_max_date = get_commodity_date_range()
    default_comm_start = date(2026, 1, 1) if date(2026, 1, 1) >= comm_min_date else comm_min_date
    
    with col1:
        selected_commodities = st.multiselect("Select commodity", options=commodities, default=commodities[0] if commodities else None, key="commodity_list", format_func=lambda x: f"{x} - {commodity_name_map.get(x, 'Unknown')}")
    
    with col2:
        st.write("")  # Spacer
    
    with col3:
        comm_min_date_filter = st.date_input("Start date", value=default_comm_start, min_value=comm_min_date, max_value=comm_max_date, key="comm_min_date")
    
    with col4:
        comm_max_date_filter = st.date_input("End date", value=comm_max_date, min_value=comm_min_date, max_value=comm_max_date, key="comm_max_date")
    
    if selected_commodities:
        df_comm = get_commodity_data(selected_commodities)
        if not df_comm.empty:
            df_comm_filtered = df_comm[(df_comm["date"].dt.date >= comm_min_date_filter) & (df_comm["date"].dt.date <= comm_max_date_filter)]
            if not df_comm_filtered.empty:
                commodity_labels = [f"{c} - {commodity_name_map.get(c, 'Unknown')}" for c in selected_commodities]
                st.subheader(f"Data for {', '.join(commodity_labels)}")
                
                fig_comm_prices = go.Figure()
                unique_commodities = df_comm_filtered.groupby("commodity_id").size().index.tolist()
                
                for commodity in unique_commodities:
                    df_commodity = df_comm_filtered[df_comm_filtered["commodity_id"] == commodity]
                    commodity_name = commodity_name_map.get(commodity, "Unknown")
                    fig_comm_prices.add_trace(go.Scatter(
                        x=df_commodity["date"], y=df_commodity["value"], mode="lines",
                        name=f"{commodity} - {commodity_name}",
                        hovertemplate=f"<b>{commodity} - {commodity_name}</b><br>Date: %{{x|%Y-%m-%d}}<br>Value: $%{{y:.2f}}<extra></extra>"
                    ))
                
                fig_comm_prices.update_layout(title="", xaxis_title="Date", yaxis_title="Price (USD)", hovermode="x unified", height=500, template="plotly_white", legend=dict(orientation="v", yanchor="top", y=0.99, xanchor="left", x=1.01))
                st.plotly_chart(fig_comm_prices, use_container_width=True)
                
                with st.expander("View Raw Data"):
                    st.dataframe(df_comm_filtered, use_container_width=True)
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Data Points", len(df_comm_filtered))
                with col2:
                    st.metric("Date Range", f"{comm_min_date_filter} to {comm_max_date_filter}")
                with col3:
                    avg_price = df_comm_filtered["value"].mean()
                    st.metric("Average Price", f"${avg_price:.2f}")
                with col4:
                    max_price = df_comm_filtered["value"].max()
                    st.metric("Maximum Price", f"${max_price:.2f}")
            else:
                st.warning("No data available for the selected date range.")
        else:
            st.warning("No data available for the selected commodities.")
