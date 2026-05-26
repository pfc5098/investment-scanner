import os
import time
import json
import logging
import requests
import pandas as pd
from datetime import datetime, timezone
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
        return data

    def get_cash_flow(self, symbol):
        params = {"function": "CASH_FLOW", "symbol": symbol}
        data = self._make_request(params)
        return data

    def get_income_statement(self, symbol):
        params = {"function": "INCOME_STATEMENT", "symbol": symbol}
        data = self._make_request(params)
        return data

def generate_html_report(df):
    os.makedirs("public", exist_ok=True)
    
    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Opportunity Scanner Report</title>
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
            .score-high {{ color: green; font-weight: bold; }}
            .score-med {{ color: orange; font-weight: bold; }}
            .score-low {{ color: red; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Opportunity Scanner Report</h1>
            <div style="margin-bottom: 15px;">
                <a href="index.html" style="{{text-decoration: none; color: #0066cc;}}">General Scan</a> | 
                <strong>Opportunity Scanner</strong>
            </div>
            <div class="timestamp">Last Updated: {timestamp}</div>
            {table}
        </div>
        
        <!-- jQuery and DataTables JS -->
        <script type="text/javascript" charset="utf8" src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
        <script type="text/javascript" charset="utf8" src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.js"></script>
        <script type="text/javascript" charset="utf8" src="https://cdn.datatables.net/buttons/2.4.2/js/dataTables.buttons.min.js"></script>
        <script type="text/javascript" charset="utf8" src="https://cdn.datatables.net/buttons/2.4.2/js/buttons.colVis.min.js"></script>
        <script type="text/javascript" charset="utf8" src="https://cdn.datatables.net/buttons/2.4.2/js/buttons.html5.min.js"></script>
        
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
                        var index = $(this).data('col-index');
                        var searchStr = $(this).val().trim();
                        if (searchStr === "") return true;
                        
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
                            
                            var min, max;
                            if (searchStr.startsWith('-')) {{
                                var midHyphen = searchStr.indexOf('-', 1);
                                min = parseFloat(searchStr.substring(0, midHyphen));
                                max = parseFloat(searchStr.substring(midHyphen + 1));
                            }} else {{
                                var parts = searchStr.split('-');
                                min = parseFloat(parts[0]);
                                max = parseFloat(parts[1]);
                            }}
                            
                            if (numericData < min || numericData > max) isVisible = false;
                        }}
                        else if (searchStr.startsWith("=")) {{
                            if (isNaN(numericData)) {{ isVisible = false; return false; }}
                            var val = parseFloat(searchStr.substring(1));
                            if (numericData !== val) isVisible = false;
                        }}
                        else {{
                            if (data[index].toLowerCase().indexOf(searchStr.toLowerCase()) === -1) {{
                                isVisible = false;
                            }}
                        }}
                    }});
                    
                    return isVisible;
                }});

                var table = $('#stockTable').DataTable({{
                    dom: 'Bfrtip',
                    buttons: [
                        'colvis',
                        {{
                            extend: 'csv',
                            text: 'Download CSV',
                            exportOptions: {{
                                orthogonal: 'export'
                            }}
                        }}
                    ],
                    orderCellsTop: true,
                    fixedHeader: true,
                    pageLength: 50,
                    order: [[4, "desc"]], // Sort by Total Score descending by default (assuming index 4)
                    columnDefs: [
                        {{
                            targets: [4], // Total Score
                            render: function(data, type, row) {{
                                if (type === 'display') {{
                                    var val = parseFloat(data);
                                    if (val >= 15) return '<span class="score-high">' + val + '</span>';
                                    if (val >= 10) return '<span class="score-med">' + val + '</span>';
                                    return '<span class="score-low">' + val + '</span>';
                                }}
                                return data;
                            }}
                        }},
                        {{
                            targets: [11, 12, 13, 14, 15, 23, 24, 28, 29, 30], // Financials / Caps (adjust indices based on final columns)
                            render: function(data, type, row) {{
                                if (type === 'display') return formatNumber(parseFloat(data));
                                return data;
                            }}
                        }},
                        {{
                            targets: [16, 17, 18, 19, 20, 21, 22, 26, 27, 31, 32, 33, 34], // Margins/Ratios/Growth (adjust indices)
                            render: function(data, type, row) {{
                                if (type === 'display' && data !== null && data !== "") {{
                                    return (parseFloat(data) * 100).toFixed(2) + "%";
                                }}
                                return data; 
                            }}
                        }}
                    ],
                    initComplete: function () {{
                        var api = this.api();
                        api.columns().eq(0).each(function (colIdx) {{
                            var headerCell = $(api.column(colIdx).header());
                            var filterCell = $('.filters th').eq(headerCell.index());
                            var title = headerCell.text();
                            
                            var isNumeric = /Score|Price|Volume|RSI|Cap|Ratio|EPS|Asset|Liability|Revenue|Margin|CF|CapEx|YoY|QoQ|High|Average/.test(title);
                            var placeholder = isNumeric ? ">50, 30-70" : "Filter...";
                            
                            $(filterCell).html('<input type="text" data-col-index="' + colIdx + '" placeholder="' + placeholder + '" />');
                            $('input', filterCell).off('keyup change').on('keyup change', function (e) {{
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
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    
    final_html = html_template.format(timestamp=current_time, table=html_table)
    
    with open("public/opportunity_report.html", "w") as f:
        f.write(final_html)
    logger.info("Successfully generated public/opportunity_report.html")

def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0

def safe_float_opt(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def calc_growth(quarterly_reports, current_idx, past_idx, key1, key2=None):
    if not quarterly_reports or len(quarterly_reports) <= max(current_idx, past_idx):
        return None
    
    current_q = quarterly_reports[current_idx]
    past_q = quarterly_reports[past_idx]
    
    val1_c = safe_float_opt(current_q.get(key1))
    val1_p = safe_float_opt(past_q.get(key1))
    
    if val1_c is None or val1_p is None:
        return None
        
    if key2:
        val2_c = safe_float_opt(current_q.get(key2))
        val2_p = safe_float_opt(past_q.get(key2))
        if val2_c is None or val2_p is None:
            return None
        val_current = val1_c - val2_c
        val_past = val1_p - val2_p
    else:
        val_current = val1_c
        val_past = val1_p
        
    if val_past == 0:
        return None
    
    return round((val_current - val_past) / abs(val_past), 4)

def calculate_scores(price, ma50, ma200, high52, rev_yoy, rev_qoq, gross_margin, op_margin, free_cf, la_ratio):
    mom_score = 0
    if price and ma50 and price > ma50: mom_score += 2
    if price and ma200 and price > ma200: mom_score += 2
    if price and high52 and price >= high52 * 0.85: mom_score += 1
    
    rev_score = 0
    if rev_yoy is not None:
        if rev_yoy > 0.40: rev_score += 4
        elif rev_yoy > 0.20: rev_score += 3
    if rev_qoq is not None and rev_qoq > 0: rev_score += 1
    if rev_score > 5: rev_score = 5
    
    prof_score = 0
    if gross_margin is not None and gross_margin > 0.30: prof_score += 1
    if op_margin is not None:
        if op_margin > 0.10: prof_score += 2
        elif op_margin > 0: prof_score += 1
    if free_cf is not None and free_cf > 0: prof_score += 2
    if prof_score > 5: prof_score = 5
    
    bs_score = 0
    if la_ratio is not None:
        if la_ratio < 0.40: bs_score += 5
        elif la_ratio < 0.60: bs_score += 4
        elif la_ratio < 0.80: bs_score += 3
            
    total_score = mom_score + rev_score + prof_score + bs_score
    return mom_score, rev_score, prof_score, bs_score, total_score

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
            overview = av_client.get_overview(symbol)
            
            if overview.get("Industry", "").upper() == "SHELL COMPANIES":
                logger.info(f"Skipping {symbol} (Shell Company)")
                continue

            market_cap = safe_float(overview.get("MarketCapitalization"))
            
            if not limit and not symbol_list_env and market_cap < 10000000000:
                logger.info(f"Skipping {symbol} (Market Cap: ${market_cap:,.0f})")
                skipped_small += 1
                continue
                
            quote = av_client.get_global_quote(symbol)
            rsi = av_client.get_rsi(symbol)
            bs = av_client.get_balance_sheet(symbol)
            cf = av_client.get_cash_flow(symbol)
            inc = av_client.get_income_statement(symbol)
            
            # Additional Overview metrics
            ma50 = safe_float_opt(overview.get("50DayMovingAverage"))
            ma200 = safe_float_opt(overview.get("200DayMovingAverage"))
            high52 = safe_float_opt(overview.get("52WeekHigh"))
            
            # Extract and Calculate metrics
            inc_q = inc.get("quarterlyReports", [])
            bs_q = bs.get("quarterlyReports", [])
            cf_q = cf.get("quarterlyReports", [])

            inc_a = inc.get("annualReports", [{}])[0] if inc.get("annualReports") else {}
            bs_a = bs.get("annualReports", [{}])[0] if bs.get("annualReports") else {}
            cf_a = cf.get("annualReports", [{}])[0] if cf.get("annualReports") else {}

            revenue = safe_float(inc_a.get("totalRevenue"))
            gross_profit = safe_float(inc_a.get("grossProfit"))
            op_income = safe_float(inc_a.get("operatingIncome"))
            net_income = safe_float(inc_a.get("netIncome"))
            
            assets = safe_float(bs_a.get("totalAssets"))
            liabilities = safe_float(bs_a.get("totalLiabilities"))
            
            op_cf = safe_float(cf_a.get("operatingCashflow"))
            capex = safe_float(cf_a.get("capitalExpenditures"))
            
            price = safe_float_opt(quote.get("Price"))
            
            gross_margin = round(gross_profit / revenue, 4) if revenue > 0 else None
            operating_margin = round(op_income / revenue, 4) if revenue > 0 else None
            net_margin = round(net_income / revenue, 4) if revenue > 0 else None
            
            rev_qoq = calc_growth(inc_q, 0, 1, "totalRevenue")
            rev_yoy = calc_growth(inc_q, 0, 4, "totalRevenue")
            
            la_ratio = round(liabilities / assets, 4) if assets > 0 else None
            free_cf = op_cf - capex if op_cf is not None and capex is not None else None
            
            mom_score, rev_score, prof_score, bs_score, total_score = calculate_scores(
                price, ma50, ma200, high52, rev_yoy, rev_qoq, gross_margin, operating_margin, free_cf, la_ratio
            )
            
            logger.info(f"Score for {symbol}: Total={total_score} (Mom={mom_score}, Rev={rev_score}, Prof={prof_score}, BS={bs_score})")
            
            row = {
                # 1. General
                "Symbol": symbol,
                "Name": overview.get("Name"),
                "Sector": overview.get("Sector"),
                "Industry": overview.get("Industry"),
                
                # 2. Scores
                "Total Score": total_score,
                "Mom Score": mom_score,
                "Rev Score": rev_score,
                "Prof Score": prof_score,
                "BS Score": bs_score,
                
                # 3. Market
                "Price": price,
                "Volume": quote.get("Volume"),
                "Market Cap": market_cap,
                "RSI (14)": rsi,
                "50d MA": ma50,
                "200d MA": ma200,
                "52w High": high52,
                
                # 4. Valuation
                "P/E Ratio": overview.get("PERatio"),
                "EPS": overview.get("EPS"),
                
                # 5. Profitability
                "Revenue": revenue,
                "Gross Margin": gross_margin,
                "Operating Margin": operating_margin,
                "Net Margin": net_margin,
                
                # 6. Income Growth
                "Rev QoQ": rev_qoq,
                "Rev YoY": rev_yoy,
                "Net Inc QoQ": calc_growth(inc_q, 0, 1, "netIncome"),
                "Net Inc YoY": calc_growth(inc_q, 0, 4, "netIncome"),
                
                # 7. Balance Sheet
                "Asset": assets,
                "Liability": liabilities,
                "L/A Ratio": la_ratio,
                
                # 8. BS Growth
                "Asset QoQ": calc_growth(bs_q, 0, 1, "totalAssets"),
                "Asset YoY": calc_growth(bs_q, 0, 4, "totalAssets"),
                
                # 9. Cash Flow
                "Operating CF": op_cf,
                "CapEx": capex,
                "Free CF": free_cf,
                
                # 10. CF Growth
                "Op CF QoQ": calc_growth(cf_q, 0, 1, "operatingCashflow"),
                "Op CF YoY": calc_growth(cf_q, 0, 4, "operatingCashflow"),
                "Free CF QoQ": calc_growth(cf_q, 0, 1, "operatingCashflow", "capitalExpenditures"),
                "Free CF YoY": calc_growth(cf_q, 0, 4, "operatingCashflow", "capitalExpenditures"),
                
                # 11. Meta
                "Last Updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            }
            results.append(row)
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            continue

    logger.info(f"Scan complete. Total processed: {len(results)}. Skipped <$10B: {skipped_small}")

    if not results:
        logger.warning("No data collected.")
        return

    df = pd.DataFrame(results)
    
    # Save raw data
    os.makedirs("data", exist_ok=True)
    csv_path = f"data/opportunity_scan_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(csv_path, index=False)
    
    # Generate HTML report
    generate_html_report(df)

if __name__ == "__main__":
    main()