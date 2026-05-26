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

    def get_time_series_daily(self, symbol):
        params = {"function": "TIME_SERIES_DAILY", "symbol": symbol, "outputsize": "full"}
        return self._make_request(params)

    def get_overview(self, symbol):
        params = {"function": "OVERVIEW", "symbol": symbol}
        return self._make_request(params)

    def get_balance_sheet(self, symbol):
        params = {"function": "BALANCE_SHEET", "symbol": symbol}
        return self._make_request(params)

    def get_cash_flow(self, symbol):
        params = {"function": "CASH_FLOW", "symbol": symbol}
        return self._make_request(params)

    def get_income_statement(self, symbol):
        params = {"function": "INCOME_STATEMENT", "symbol": symbol}
        return self._make_request(params)

def filter_quarterly_reports(reports, target_date):
    filtered = []
    for r in reports:
        dt_str = r.get("fiscalDateEnding")
        if dt_str and dt_str <= target_date:
            filtered.append(r)
    return filtered

def generate_html_report(df):
    os.makedirs("public", exist_ok=True)
    
    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Point-in-Time Backtest Report</title>
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
            <h1>Point-in-Time Backtest Report</h1>
            <div class="timestamp">Backtest Target Date: {target_date} | Run Time: {timestamp}</div>
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
            
                // Custom filtering function
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
                    buttons: ['colvis', {{extend: 'csv', text: 'Download CSV'}}],
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
                            targets: [10, 11, 12, 13, 14, 22, 23, 27, 28, 29], // Financials / Caps (adjust indices based on final columns)
                            render: function(data, type, row) {{
                                if (type === 'display') return formatNumber(parseFloat(data));
                                return data;
                            }}
                        }},
                        {{
                            targets: [15, 16, 17, 18, 19, 20, 21, 25, 26, 30, 31, 32, 33], // Margins/Ratios/Growth (adjust indices)
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
                            
                            var isNumeric = /Score|Price|Volume|Cap|Ratio|EPS|Asset|Liability|Revenue|Margin|CF|CapEx|YoY|QoQ|High|Average/.test(title);
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
    target_date = os.environ.get("TARGET_DATE", "2025-06-30")
    html_table = df.to_html(index=False, table_id="stockTable", classes='display', border=0)
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    
    final_html = html_template.format(target_date=target_date, timestamp=current_time, table=html_table)
    
    with open("public/backtest_report.html", "w") as f:
        f.write(final_html)
    logger.info("Successfully generated public/backtest_report.html")

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
    target_date = os.environ.get("TARGET_DATE", "2025-06-30")
    logger.info(f"Running point-in-time backtest for date: {target_date}")
    
    symbol_list_env = os.environ.get("SYMBOL_LIST", "MU")
    symbols = [s.strip() for s in symbol_list_env.split(",") if s.strip()]
    logger.info(f"Using provided SYMBOL_LIST: {symbols}")

    results = []
    
    for i, symbol in enumerate(symbols):
        logger.info(f"Processing [{i+1}/{len(symbols)}]: {symbol}")
        try:
            # 1. Fetch historical prices
            time_series = av_client.get_time_series_daily(symbol)
            ts_data = time_series.get("Time Series (Daily)", {})
            
            # Determine target_date dynamically if requested, else use global target_date
            dynamic_mode = os.environ.get("DYNAMIC_INFLECTION", "true").lower() == "true"
            symbol_target_date = target_date
            
            all_dates_asc = sorted(ts_data.keys())
            if dynamic_mode:
                valid_dates_for_inflection = [d for d in all_dates_asc if d >= "2024-01-01"]
                max_ret = -1
                best_date = None
                for j in range(20, len(valid_dates_for_inflection)):
                    d_now = valid_dates_for_inflection[j]
                    d_prev = valid_dates_for_inflection[j-20]
                    p_now = safe_float(ts_data[d_now]["4. close"])
                    p_prev = safe_float(ts_data[d_prev]["4. close"])
                    if p_prev > 0:
                        ret = (p_now - p_prev) / p_prev
                        if ret > max_ret:
                            max_ret = ret
                            best_date = d_now
                
                if best_date:
                    symbol_target_date = best_date
                    logger.info(f"Dynamic inflection point for {symbol}: {symbol_target_date} (20-day return: {max_ret*100:.1f}%)")
                else:
                    logger.warning(f"Could not find dynamic inflection point for {symbol}. Using global target.")
            
            # Filter dates and sort descending relative to the symbol's target date
            valid_dates = sorted([d for d in ts_data.keys() if d <= symbol_target_date], reverse=True)
            
            if not valid_dates:
                logger.warning(f"No price data available for {symbol} on or before {symbol_target_date}")
                continue
                
            # Get latest price as of target date
            latest_valid_date = valid_dates[0]
            price = safe_float_opt(ts_data[latest_valid_date].get("4. close"))
            
            # Calculate MAs and Highs
            prices_last_50 = [safe_float(ts_data[d]["4. close"]) for d in valid_dates[:50]]
            prices_last_200 = [safe_float(ts_data[d]["4. close"]) for d in valid_dates[:200]]
            prices_last_252 = [safe_float(ts_data[d]["2. high"]) for d in valid_dates[:252]] # 52w high roughly 252 trading days
            
            ma50 = sum(prices_last_50) / len(prices_last_50) if prices_last_50 else None
            ma200 = sum(prices_last_200) / len(prices_last_200) if prices_last_200 else None
            high52 = max(prices_last_252) if prices_last_252 else None
            
            # 2. Get Overview for basic info
            overview = av_client.get_overview(symbol)

            # 3. Fetch Financials and filter by TARGET_DATE
            bs = av_client.get_balance_sheet(symbol)
            cf = av_client.get_cash_flow(symbol)
            inc = av_client.get_income_statement(symbol)
            
            inc_q = filter_quarterly_reports(inc.get("quarterlyReports", []), symbol_target_date)
            bs_q = filter_quarterly_reports(bs.get("quarterlyReports", []), symbol_target_date)
            cf_q = filter_quarterly_reports(cf.get("quarterlyReports", []), symbol_target_date)
            
            if not inc_q or not bs_q or not cf_q:
                logger.warning(f"Missing fundamental data for {symbol} on or before {symbol_target_date}")
                continue

            # Latest fundamental data as of TARGET_DATE
            latest_inc = inc_q[0]
            latest_bs = bs_q[0]
            latest_cf = cf_q[0]

            revenue = safe_float(latest_inc.get("totalRevenue"))
            gross_profit = safe_float(latest_inc.get("grossProfit"))
            op_income = safe_float(latest_inc.get("operatingIncome"))
            net_income = safe_float(latest_inc.get("netIncome"))
            
            assets = safe_float(latest_bs.get("totalAssets"))
            liabilities = safe_float(latest_bs.get("totalLiabilities"))
            
            op_cf = safe_float(latest_cf.get("operatingCashflow"))
            capex = safe_float(latest_cf.get("capitalExpenditures"))
            
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
            
            logger.info(f"Score for {symbol} as of {symbol_target_date}: Total={total_score} (Mom={mom_score}, Rev={rev_score}, Prof={prof_score}, BS={bs_score})")
            
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
                
                # 3. Market Point-in-Time
                "Price": price,
                "50d MA": ma50,
                "200d MA": ma200,
                "52w High": high52,
                
                # 4. Valuation (We omit point-in-time P/E since it's hard to accurately calculate dynamically)
                "P/E Ratio": None,
                "EPS": None,
                
                # 5. Profitability Point-in-Time
                "Revenue": revenue,
                "Gross Margin": gross_margin,
                "Operating Margin": operating_margin,
                "Net Margin": net_margin,
                
                # 6. Income Growth Point-in-Time
                "Rev QoQ": rev_qoq,
                "Rev YoY": rev_yoy,
                "Net Inc QoQ": calc_growth(inc_q, 0, 1, "netIncome"),
                "Net Inc YoY": calc_growth(inc_q, 0, 4, "netIncome"),
                
                # 7. Balance Sheet Point-in-Time
                "Asset": assets,
                "Liability": liabilities,
                "L/A Ratio": la_ratio,
                
                # 8. BS Growth Point-in-Time
                "Asset QoQ": calc_growth(bs_q, 0, 1, "totalAssets"),
                "Asset YoY": calc_growth(bs_q, 0, 4, "totalAssets"),
                
                # 9. Cash Flow Point-in-Time
                "Operating CF": op_cf,
                "CapEx": capex,
                "Free CF": free_cf,
                
                # 10. CF Growth Point-in-Time
                "Op CF QoQ": calc_growth(cf_q, 0, 1, "operatingCashflow"),
                "Op CF YoY": calc_growth(cf_q, 0, 4, "operatingCashflow"),
                "Free CF QoQ": calc_growth(cf_q, 0, 1, "operatingCashflow", "capitalExpenditures"),
                "Free CF YoY": calc_growth(cf_q, 0, 4, "operatingCashflow", "capitalExpenditures"),
                
                # 11. Meta
                "Backtest Date": symbol_target_date
            }
            results.append(row)
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            continue

    logger.info(f"Backtest complete. Total processed: {len(results)}.")

    if not results:
        logger.warning("No data collected.")
        return

    df = pd.DataFrame(results)
    
    # Save raw data
    os.makedirs("data", exist_ok=True)
    csv_path = f"data/backtest_scan_{target_date}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(csv_path, index=False)
    
    # Generate HTML report
    generate_html_report(df)

if __name__ == "__main__":
    main()