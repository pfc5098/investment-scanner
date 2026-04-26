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
        return data

    def get_balance_sheet(self, symbol):
        params = {"function": "BALANCE_SHEET", "symbol": symbol}
        data = self._make_request(params)
        reports = data.get("annualReports", [])
        return reports[0] if reports else {}

    def get_cash_flow(self, symbol):
        params = {"function": "CASH_FLOW", "symbol": symbol}
        data = self._make_request(params)
        reports = data.get("annualReports", [])
        return reports[0] if reports else {}

    def get_income_statement(self, symbol):
        params = {"function": "INCOME_STATEMENT", "symbol": symbol}
        data = self._make_request(params)
        reports = data.get("annualReports", [])
        return reports[0] if reports else {}

def generate_html_report(df):
    os.makedirs("public", exist_ok=True)
    
    # Add basic styling and DataTables integration with ColVis
    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Daily Stock Scan</title>
        <!-- DataTables CSS -->
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.css">
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/buttons/2.4.2/css/buttons.dataTables.min.css">
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; padding: 20px; background-color: #f5f5f7; color: #333; }}
            h1 {{ color: #1d1d1f; }}
            .container {{ max-width: 98%; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .timestamp {{ color: #86868b; margin-bottom: 20px; font-size: 0.9em; }}
            table {{ width: 100%; font-size: 0.85em; }}
            th, td {{ padding: 8px 10px; text-align: left; }}
            thead th {{ background-color: #f8f9fa; font-weight: 600; color: #1d1d1f; }}
            thead input {{ width: 100%; padding: 3px; box-sizing: border-box; margin-top: 5px; font-size: 0.8em; font-weight: normal; }}
            .dt-buttons {{ margin-bottom: 15px; }}
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
        <script type="text/javascript" charset="utf8" src="https://cdn.datatables.net/buttons/2.4.2/js/dataTables.buttons.min.js"></script>
        <script type="text/javascript" charset="utf8" src="https://cdn.datatables.net/buttons/2.4.2/js/buttons.colVis.min.js"></script>
        
        <script>
            function formatNumber(num) {{
                if (num === null || isNaN(num)) return "-";
                if (Math.abs(num) >= 1.0e+12) return (num / 1.0e+12).toFixed(2) + " T";
                if (Math.abs(num) >= 1.0e+09) return (num / 1.0e+09).toFixed(2) + " B";
                if (Math.abs(num) >= 1.0e+06) return (num / 1.0e+06).toFixed(2) + " M";
                return num.toLocaleString(undefined, {{maximumFractionDigits: 2}});
            }}

            $(document).ready(function() {{
                // Setup - add a text input to each header cell
                $('#stockTable thead tr')
                    .clone(true)
                    .addClass('filters')
                    .appendTo('#stockTable thead');
            
                // Custom filtering function for mathematical operators
                $.fn.dataTable.ext.search.push(function(settings, data, dataIndex, rowData) {{
                    var isVisible = true;
                    
                    $('#stockTable .filters input').each(function() {{
                        var index = $(this).parent().index();
                        var searchStr = $(this).val().trim();
                        if (searchStr === "") return true;
                        
                        // We use the raw original data (rowData) from pandas to do pure numeric filtering
                        var cellDataRaw = rowData[index];
                        var numericData = parseFloat(cellDataRaw);
                        
                        if (searchStr.match(/^[<>]=?\\s*[\\-]?\\d+\\.?\\d*$/)) {{
                            if (isNaN(numericData)) {{ isVisible = false; return false; }}
                            var operator = searchStr.match(/^[<>]=?/)[0];
                            var val = parseFloat(searchStr.substring(operator.length));
                            
                            if (operator === '>' && !(numericData > val)) isVisible = false;
                            if (operator === '>=' && !(numericData >= val)) isVisible = false;
                            if (operator === '<' && !(numericData < val)) isVisible = false;
                            if (operator === '<=' && !(numericData <= val)) isVisible = false;
                        }} 
                        else if (searchStr.match(/^[\\-]?\\d+\\.?\\d*\\s*-\\s*[\\-]?\\d+\\.?\\d*$/)) {{
                            if (isNaN(numericData)) {{ isVisible = false; return false; }}
                            var parts = searchStr.split('-');
                            // Handle negative numbers in range correctly
                            var minStr = parts[0].trim() === "" && searchStr.startsWith('-') ? "-" + parts[1] : parts[0];
                            var maxStr = parts[0].trim() === "" && searchStr.startsWith('-') ? parts[2] : parts[1];
                            
                            var min = parseFloat(minStr);
                            var max = parseFloat(maxStr);
                            if (numericData < min || numericData > max) isVisible = false;
                        }}
                        else if (searchStr.startsWith("=")) {{
                            if (isNaN(numericData)) {{ isVisible = false; return false; }}
                            var val = parseFloat(searchStr.substring(1));
                            if (numericData !== val) isVisible = false;
                        }}
                        else {{
                            // Text search fallback - use display data for this
                            if (data[index].toLowerCase().indexOf(searchStr.toLowerCase()) === -1) {{
                                isVisible = false;
                            }}
                        }}
                    }});
                    
                    return isVisible;
                }});

                var table = $('#stockTable').DataTable({{
                    dom: 'Bfrtip',
                    buttons: ['colvis'],
                    orderCellsTop: true,
                    fixedHeader: true,
                    pageLength: 50,
                    columnDefs: [
                        {{
                            targets: [1, 4, 9, 10, 12, 16, 17, 18], // Financials
                            render: function(data, type, row) {{
                                if (type === 'display') return formatNumber(parseFloat(data));
                                return data; // Raw float for sort/filter
                            }}
                        }},
                        {{
                            targets: [2], // Volume
                            render: function(data, type, row) {{
                                if (type === 'display') return parseFloat(data).toLocaleString();
                                return data; // Raw float for sort/filter
                            }}
                        }},
                        {{
                            targets: [11, 13, 14, 15], // Margins/Ratios
                            render: function(data, type, row) {{
                                if (type === 'display' && data !== null && data !== "") {{
                                    return (parseFloat(data) * 100).toFixed(2) + "%";
                                }}
                                return data; // Raw float for sort/filter
                            }}
                        }}
                    ],
                    initComplete: function () {{
                        var api = this.api();
                        api.columns().eq(0).each(function (colIdx) {{
                            var cell = $('.filters th').eq($(api.column(colIdx).header()).index());
                            var title = $(api.column(colIdx).header()).text();
                            
                            var isNumeric = /Price|Volume|RSI|Cap|Ratio|EPS|Asset|Liability|Revenue|Margin|CF|CapEx/.test(title);
                            var placeholder = isNumeric ? ">50, 30-70" : "Filter...";
                            
                            $(cell).html('<input type="text" placeholder="' + placeholder + '" />');
                            $('input', cell).off('keyup change').on('keyup change', function (e) {{
                                table.draw();
                            }});
                        }});
                    }},
                }});
            }});
        </script>
    </body>
    </html>
    """
    
    html_table = df.to_html(index=False, table_id="stockTable", classes='display', border=0)
    current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    
    final_html = html_template.format(timestamp=current_time, table=html_table)
    
    with open("public/index.html", "w") as f:
        f.write(final_html)
    logger.info("Successfully generated public/index.html")

def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0

def main():
    api_key = os.environ.get("ALPHAVANTAGE_API_KEY")

    if not api_key:
        logger.error("Missing ALPHAVANTAGE_API_KEY environment variable.")
        return

    av_client = AlphaVantageClient(api_key=api_key)
    
    limit = os.environ.get("SYMBOL_LIMIT")
    symbol_list_env = os.environ.get("SYMBOL_LIST")
    
    if symbol_list_env:
        symbols = [s.strip() for s in symbol_list_env.split(",") if s.strip()]
        logger.info(f"Using provided SYMBOL_LIST: {symbols}")
        total_listings = len(symbols)
    else:
        symbols = av_client.get_active_listings()
        total_listings = len(symbols)
        if limit:
            symbols = symbols[:int(limit)]
            logger.info(f"Limiting to {limit} symbols for testing.")

    results = []
    skipped_small = 0
    
    for i, symbol in enumerate(symbols):
        logger.info(f"Processing [{i+1}/{len(symbols)}]: {symbol}")
        try:
            # 1. Get Overview FIRST to filter by Market Cap
            overview = av_client.get_overview(symbol)
            market_cap = safe_float(overview.get("MarketCapitalization"))
            
            # Filter: Market Cap > $250 Million
            # Bypass filter if we are explicitly testing a symbol list or limit
            if not limit and not symbol_list_env and market_cap < 250000000:
                logger.info(f"Skipping {symbol} (Market Cap: ${market_cap:,.0f})")
                skipped_small += 1
                continue
            
            # 2. If it passes, get the rest
            quote = av_client.get_global_quote(symbol)
            rsi = av_client.get_rsi(symbol)
            bs = av_client.get_balance_sheet(symbol)
            cf = av_client.get_cash_flow(symbol)
            inc = av_client.get_income_statement(symbol)
            
            # Extract and Calculate metrics
            revenue = safe_float(inc.get("totalRevenue"))
            gross_profit = safe_float(inc.get("grossProfit"))
            op_income = safe_float(inc.get("operatingIncome"))
            net_income = safe_float(inc.get("netIncome"))
            
            assets = safe_float(bs.get("totalAssets"))
            liabilities = safe_float(bs.get("totalLiabilities"))
            
            op_cf = safe_float(cf.get("operatingCashflow"))
            capex = safe_float(cf.get("capitalExpenditures"))
            
            row = {
                "Symbol": symbol,
                "Price": quote.get("Price"),
                "Volume": quote.get("Volume"),
                "RSI (14)": rsi,
                "Market Cap": market_cap,
                "P/E Ratio": overview.get("PERatio"),
                "EPS": overview.get("EPS"),
                "Sector": overview.get("Sector"),
                "Industry": overview.get("Industry"),
                "Asset": assets,
                "Liability": liabilities,
                "L/A Ratio": round(liabilities / assets, 4) if assets > 0 else None,
                "Revenue": revenue,
                "Gross Margin": round(gross_profit / revenue, 4) if revenue > 0 else None,
                "Operating Margin": round(op_income / revenue, 4) if revenue > 0 else None,
                "Net Margin": round(net_income / revenue, 4) if revenue > 0 else None,
                "Operating CF": op_cf,
                "CapEx": capex,
                "Free CF": op_cf - capex,
                "Last Updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            }
            results.append(row)
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            continue

    logger.info(f"Scan complete. Total processed: {len(results)}. Skipped <$100M: {skipped_small}")
    if total_listings > 0:
        logger.info(f"Post-filter percentage: {(len(results) / len(symbols)) * 100:.2f}% of processed symbols passed.")

    if not results:
        logger.warning("No data collected.")
        return

    df = pd.DataFrame(results)
    
    # Save raw data
    os.makedirs("data", exist_ok=True)
    csv_path = f"data/daily_scan_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(csv_path, index=False)
    
    # Generate HTML report
    generate_html_report(df)

if __name__ == "__main__":
    main()