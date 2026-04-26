import os
import time
import json
import logging
import requests
import pandas as pd
from datetime import datetime
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
        
        from io import StringIO
        df = pd.read_csv(StringIO(response.text))
        
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
        if "Technical Analysis: RSI" in data:
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

def generate_html_report(df):
    os.makedirs("public", exist_ok=True)
    
    # Add basic styling and DataTables integration
    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Daily Stock Scan</title>
        <!-- DataTables CSS -->
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.css">
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; padding: 20px; background-color: #f5f5f7; color: #333; }}
            h1 {{ color: #1d1d1f; }}
            .container {{ max-width: 95%; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .timestamp {{ color: #86868b; margin-bottom: 20px; font-size: 0.9em; }}
            table {{ width: 100%; font-size: 0.9em; }}
            th, td {{ padding: 10px 12px; text-align: left; }}
            thead th {{ background-color: #f8f9fa; font-weight: 600; color: #1d1d1f; }}
            thead input {{ width: 100%; padding: 3px; box-sizing: border-box; margin-top: 5px; font-size: 0.8em; font-weight: normal; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Daily US Stocks Scan</h1>
            <div class="timestamp">Last Updated: {timestamp}</div>
            {table}
        </div>
        
        <!-- jQuery and DataTables JS -->
        <script type="text/javascript" charset="utf8" src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
        <script type="text/javascript" charset="utf8" src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.js"></script>
        
        <script>
            $(document).ready(function() {{
                // Setup - add a text input to each footer cell (we'll prepend it to the header later)
                $('#stockTable thead tr')
                    .clone(true)
                    .addClass('filters')
                    .appendTo('#stockTable thead');
            
                var table = $('#stockTable').DataTable({{
                    orderCellsTop: true,
                    fixedHeader: true,
                    pageLength: 50,
                    initComplete: function () {{
                        var api = this.api();
             
                        // For each column
                        api
                            .columns()
                            .eq(0)
                            .each(function (colIdx) {{
                                // Set the header cell to contain the input element
                                var cell = $('.filters th').eq(
                                    $(api.column(colIdx).header()).index()
                                );
                                var title = $(cell).text();
                                $(cell).html('<input type="text" placeholder="Filter..." />');
             
                                // On every keypress in this input
                                $(
                                    'input',
                                    $('.filters th').eq($(api.column(colIdx).header()).index())
                                )
                                    .off('keyup change')
                                    .on('change', function (e) {{
                                        // Get the search value
                                        $(this).attr('title', $(this).val());
                                        var regexr = '({{search}})'; //$(this).parents('th').find('select').val();
             
                                        var cursorPosition = this.selectionStart;
                                        // Search the column for that value
                                        api
                                            .column(colIdx)
                                            .search(
                                                this.value != ''
                                                    ? regexr.replace('{{search}}', '(((' + this.value + ')))')
                                                    : '',
                                                this.value != '',
                                                this.value == ''
                                            )
                                            .draw();
                                    }})
                                    .on('keyup', function (e) {{
                                        e.stopPropagation();
             
                                        $(this).trigger('change');
                                        $(this)
                                            .focus()[0]
                                            .setSelectionRange(cursorPosition, cursorPosition);
                                    }});
                            }});
                    }},
                }});
            }});
        </script>
    </body>
    </html>
    """
    
    # Format the table and inject an ID
    html_table = df.to_html(index=False, table_id="stockTable", classes='display', border=0)
    current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    
    final_html = html_template.format(timestamp=current_time, table=html_table)
    
    with open("public/index.html", "w") as f:
        f.write(final_html)
    logger.info("Successfully generated public/index.html")

def main():
    api_key = os.environ.get("ALPHAVANTAGE_API_KEY")

    if not api_key:
        logger.error("Missing ALPHAVANTAGE_API_KEY environment variable.")
        return

    av_client = AlphaVantageClient(api_key=api_key)
    
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
            continue

    if not results:
        logger.warning("No data collected.")
        return

    df = pd.DataFrame(results)
    
    # Save raw data
    os.makedirs("data", exist_ok=True)
    csv_path = f"data/daily_scan_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(csv_path, index=False)
    logger.info(f"Successfully saved results to {csv_path}")
    
    # Generate HTML report
    generate_html_report(df)

if __name__ == "__main__":
    main()