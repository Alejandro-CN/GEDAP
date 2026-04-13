# GEDAP - Git • ETL • Databases • APIs • Python
A full-stack data engineering project demonstrating modern best practices for building scalable ETL pipelines. GEDAP fetches, transforms, and loads financial market data from multiple sources into a centralized SQLite database, with interactive dashboards for exploration and analysis.

## Features
- Multi-source data integration — Alpha Vantage API, CoinGecko API, and Investing.com
- Interactive dashboards — Streamlit + Plotly visualizations with multi-symbol comparison, filtering, and real-time database queries
- Robust data management — SQLite with validation, deduplication, historical preservation, and strategic indexing
- Modular architecture — Clean separation of concerns, reusable utilities, easy to extend

## Tech Stack
- 🐍 **Python 3.11** - Primary development language
- 🗄️ **SQLite3** - Relational database for data storage
- 📊 **Streamlit** - Interactive web framework for dashboards
- 📈 **Plotly** - Advanced data visualization
- 🐼 **Pandas** - Data manipulation and analysis
- 📡 **APIs** - Alpha Vantage, CoinGecko
- 🕸️ **Web-scraping** - Investing.com 
- 🐱 **Git & GitHub** - Version control

## Project Structure
GEDAP/
├── scripts/
│   ├── insert_*.py (ETL scripts)     # Data collection from various sources
│   ├── streamlit_dashboard.py        # Interactive dashboard
│   └── utils/                        # Shared utility functions
└── GEDAP_DB.db                       # SQLite database

## Installation & Setup
**Prerequisites:**
- Python 3.7+
- pip or conda

**Steps:**
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd GEDAP
   ```
2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Configure API Keys:
   - Get free API keys from alphavantage.co and coingecko.com and set them as environment variables. 

## Usage
**Running Data Collection:**
```bash
python scripts/insert_alphav_stocks_daily.py   # Stocks
python scripts/insert_coingecko_market_data.py # Crypto
python scripts/insert_alphav_fx_daily.py       # Forex
# Run any insert_*.py script to collect data
```
**Launching the Dashboard:**
```bash
streamlit run scripts/streamlit_dashboard.py
# Opens at http://localhost:8501
```

## Future Improvements
- 🐳 **Docker** — Containerized deployment
- 🗓️ **Scheduling** - Apache Airflow or Celery for automated ETL runs
- ☁️ **Cloud** - AWS/Azure integration
- 🔐 **Security** - Secrets management and environment-based config
- 📧 **Alerting** - Notifications for significant price movements