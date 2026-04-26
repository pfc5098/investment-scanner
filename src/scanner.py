import os
import time
import json
import logging
import requests
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AlphaVantageClient:
    def __init__(self, api_key, max_req_per_min=75):
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"
        self.delay = 60.0 / max_req_per_min
        self.last_request_time = 0

    def _wait(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_request_time = time.time()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _make_request(self, params):
        self._wait()
        params['apikey'] = self.api_key
        response = requests.get(self.base_url, params=params)
        response.raise_for_status()
        
        # Alpha Vantage returns 200 OK with Information message if API rate limit exceeded
        # But in premium this shouldn't happen if we respect the pacing. Still, good to check.
        data = response.json() if 'json' in response.headers.get('Content-Type', '') else response.text
        if isinstance(data, dict) and "Information" in data and "rate limit" in data["Information"].lower():
            logger.warning("Rate limit hit from API response, backing off...")
            raise Exception("Rate limit hit")
        
        return data

    def get_active_listings(self):
        logger.info("Fetching active US equity listings...")
        self._wait()
        params = {
            "function": "LISTING_STATUS",
            "state": "active",
            "apikey": self.api_key
        }
        response = requests.get(self.base_url, params=params)
        response.raise_for_status()
        
        # The response is a CSV
        from io import StringIO
        df = pd.read_csv(StringIO(response.text))
        
        # Filter for US Equities (usually standard stock)
        us_equities = df[df['assetType'] == 'Stock']
        logger.info(f"Found {len(us_equities)} active US Equities.")
        return us_equities['symbol'].tolist()

    def get_global_quote(self, symbol):
        params = {"function": "GLOBAL_QUOTE", "symbol": symbol}
        data = self._make_request(params)
        quote = data.get("Global Quote", {})
        return {
            "Price": quote.get("05. price", None),
            "Volume": quote.get("06. volume", None),
            "Latest Trading Day": quote.get("07. latest trading day", None)
        }

    def get_rsi(self, symbol):
        params = {
            "function": "RSI",
            "symbol": symbol,
            "interval": "daily",
            "time_period": 14,
            "series_type": "close"
        }
        data = self._make_request(params)
        # Handle cases where RSI is not available
        if "Technical Analysis: RSI" in data:
            # Get the most recent RSI
            dates = list(data["Technical Analysis: RSI"].keys())
            if dates:
                latest_date = dates[0]
                return data["Technical Analysis: RSI"][latest_date].get("RSI")
        return None

    def get_overview(self, symbol):
        params = {"function": "OVERVIEW", "symbol": symbol}
        data = self._make_request(params)
        return {
            "MarketCap": data.get("MarketCapitalization", None),
            "PERatio": data.get("PERatio", None),
            "EPS": data.get("EPS", None),
            "Sector": data.get("Sector", None),
            "Industry": data.get("Industry", None)
        }

class GoogleSheetsClient:
    def __init__(self, service_account_json, sheet_url):
        self.scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds_dict = json.loads(service_account_json)
        self.credentials = Credentials.from_service_account_info(creds_dict, scopes=self.scopes)
        self.client = gspread.authorize(self.credentials)
        self.sheet_url = sheet_url

    def update_sheet(self, df, worksheet_name="Data"):
        logger.info(f"Updating Google Sheet at {self.sheet_url}")
        spreadsheet = self.client.open_by_url(self.sheet_url)
        
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols="20")
            
        # Clear existing data
        worksheet.clear()
        
        # Fill NaN values with empty string for JSON serialization
        df = df.fillna("")
        
        # Convert DataFrame to list of lists for gspread
        data = [df.columns.values.tolist()] + df.values.tolist()
        
        worksheet.update('A1', data)
        logger.info("Successfully updated Google Sheet.")

def main():
    api_key = os.environ.get("ALPHAVANTAGE_API_KEY")
    service_account_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
    sheet_url = os.environ.get("GOOGLE_SHEET_URL")

    if not api_key:
        logger.error("Missing ALPHAVANTAGE_API_KEY environment variable.")
        return

    av_client = AlphaVantageClient(api_key=api_key)
    
    if service_account_json and sheet_url:
        gs_client = GoogleSheetsClient(service_account_json=service_account_json, sheet_url=sheet_url)
    else:
        gs_client = None
        logger.info("Google Sheets credentials not provided. Will save results to local CSV.")

    # For testing purposes, allow limiting the number of symbols
    limit = os.environ.get("SYMBOL_LIMIT")
    
    symbols = av_client.get_active_listings()
    if limit:
        symbols = symbols[:int(limit)]
        logger.info(f"Limiting to {limit} symbols for testing.")

    results = []
    total = len(symbols)
    
    for i, symbol in enumerate(symbols):
        logger.info(f"Processing [{i+1}/{total}]: {symbol}")
        try:
            quote = av_client.get_global_quote(symbol)
            rsi = av_client.get_rsi(symbol)
            overview = av_client.get_overview(symbol)
            
            row = {
                "symbol": symbol,
                "price": quote.get("Price"),
                "volume": quote.get("Volume"),
                "rsi_14": rsi,
                "market_cap": overview.get("MarketCap"),
                "pe_ratio": overview.get("PERatio"),
                "eps": overview.get("EPS"),
                "sector": overview.get("Sector"),
                "industry": overview.get("Industry"),
                "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            }
            results.append(row)
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            # Continue with next symbol instead of failing entire run
            continue

    if not results:
        logger.warning("No data collected.")
        return

    df = pd.DataFrame(results)
    
    if gs_client:
        # Try to write to a new tab per day or overwrite "Data"
        # Overwriting "Data" is usually better for connected dashboards
        gs_client.update_sheet(df, worksheet_name="Data")
    else:
        os.makedirs("data", exist_ok=True)
        csv_path = f"data/daily_scan_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(csv_path, index=False)
        logger.info(f"Successfully saved results to {csv_path}")

if __name__ == "__main__":
    main()