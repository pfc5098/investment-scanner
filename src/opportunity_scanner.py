import os
import time
import json
import logging
import requests
import pandas as pd
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_exponential

from data_fetcher import build_dataset

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
            /* Freeze the header row(s) and the first column (Symbol) while scrolling. */
            #stockTable {{ border-collapse: separate; border-spacing: 0; }}
            #stockTable thead th {{ position: -webkit-sticky; position: sticky; z-index: 2; background-color: #f8f9fa; }}
            #stockTable thead tr:first-child th {{ top: 0; }}
            #stockTable thead tr.filters th {{ top: 38px; }}
            #stockTable th:first-child, #stockTable td:first-child {{ position: -webkit-sticky; position: sticky; left: 0; z-index: 1; background-color: #fff; box-shadow: 1px 0 0 #e5e5e7; }}
            #stockTable thead th:first-child {{ z-index: 3; background-color: #f8f9fa; }}
            thead input {{ width: 100%; padding: 3px; box-sizing: border-box; margin-top: 5px; font-size: 0.8em; font-weight: normal; }}
            .dt-buttons {{ margin-bottom: 15px; }}
            .score-high {{ color: green; font-weight: bold; }}
            .score-med {{ color: orange; font-weight: bold; }}
            .score-low {{ color: red; }}
            .nav {{ margin-bottom: 15px; }}
            .nav a {{ text-decoration: none; color: #0066cc; }}
            .legend {{ background: #f8f9fa; border: 1px solid #e5e5e7; border-radius: 8px; padding: 12px 16px; margin-bottom: 20px; font-size: 0.85em; }}
            .legend summary {{ cursor: pointer; font-weight: 600; color: #1d1d1f; }}
            .legend ul {{ margin: 10px 0 4px 0; padding-left: 18px; }}
            .legend li {{ margin-bottom: 6px; line-height: 1.45; }}
            .legend .name {{ font-weight: 600; }}
            .legend .max {{ color: #86868b; font-weight: normal; }}
            /* Phone-friendly layout: reclaim horizontal space and shrink oversized chrome. */
            @media (max-width: 640px) {{
                body {{ padding: 8px; }}
                .container {{ padding: 12px; max-width: 100%; }}
                h1 {{ font-size: 1.45em; }}
                table {{ font-size: 0.8em; }}
                th, td {{ padding: 6px 8px; }}
                .legend {{ font-size: 0.8em; padding: 10px 12px; }}
                .dt-buttons, div.dataTables_filter {{ float: none; text-align: left; margin-bottom: 10px; }}
                div.dataTables_filter input {{ width: 65%; }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Opportunity Scanner Report</h1>
            <div class="nav">
                <a href="index.html">General Scan</a> |
                <strong>Opportunity Scanner</strong>
            </div>
            <div class="timestamp">Last Updated: {timestamp}</div>
            <details class="legend">
                <summary>How the Total Score works (0&ndash;20)</summary>
                <ul>
                    <li><span class="name">Mom Score</span> <span class="max">(0&ndash;5)</span> &mdash; Price momentum. +2 if price is above its 50-day moving average, +2 if above its 200-day average, +1 if within 15% of the 52-week high.</li>
                    <li><span class="name">Rev Score</span> <span class="max">(0&ndash;5)</span> &mdash; Revenue growth. +4 if year-over-year revenue is up &gt;40% (+3 if &gt;20%), and +1 if the latest quarter grew vs. the prior quarter.</li>
                    <li><span class="name">Prof Score</span> <span class="max">(0&ndash;5)</span> &mdash; Profitability. +1 for gross margin &gt;30%, +2 for operating margin &gt;10% (+1 if just positive), +2 for positive free cash flow.</li>
                    <li><span class="name">BS Score</span> <span class="max">(0&ndash;5)</span> &mdash; Balance-sheet strength via the liabilities-to-assets ratio: +5 if &lt;40%, +4 if &lt;60%, +3 if &lt;80%.</li>
                </ul>
                <div class="max">Total Score is the sum of the four. In the table it is colored <span style="color:green;font-weight:bold;">green</span> at &ge;15 and <span style="color:orange;font-weight:bold;">orange</span> at &ge;10.</div>
            </details>
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
                    pageLength: 50,
                    order: [[{score_idx}, "desc"]], // Sort by Total Score descending by default
                    columnDefs: [
                        {{
                            targets: [{score_idx}], // Total Score
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
                            targets: {number_targets}, // Prices / caps / dollar magnitudes (B/T)
                            render: function(data, type, row) {{
                                if (type === 'display') return formatNumber(parseFloat(data));
                                return data;
                            }}
                        }},
                        {{
                            targets: {percent_targets}, // Margins / growth rates (x100 + %)
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

                        // Pin the filter row directly beneath the (variable-height) header row.
                        function syncStickyOffset() {{
                            var h = $('#stockTable thead tr:first-child th').outerHeight();
                            $('#stockTable thead tr.filters th').css('top', h + 'px');
                        }}
                        syncStickyOffset();
                        $(window).on('resize', syncStickyOffset);
                    }},
                }});
            }});
        </script>
    </body>
    </html>
    """
    
    html_table = df.to_html(index=False, table_id="stockTable", classes='display', border=0)
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Resolve formatter target columns by NAME so they never drift when columns
    # are added/reordered. Dollar-magnitude/price columns -> formatNumber (B/T);
    # margins & growth rates -> x100 + "%".
    cols = list(df.columns)

    def col_indices(names):
        return [cols.index(n) for n in names if n in cols]

    number_cols = [
        "Price", "Volume", "Market Cap", "RSI (14)", "50d MA", "200d MA",
        "52w High", "P/E Ratio", "EPS", "Revenue", "Asset", "Liability",
        "L/A Ratio", "Operating CF", "CapEx", "Free CF",
    ]
    percent_cols = [
        "Gross Margin", "Operating Margin", "Net Margin",
        "Rev QoQ", "Rev YoY", "Net Inc QoQ", "Net Inc YoY",
        "Asset QoQ", "Asset YoY",
        "Op CF QoQ", "Op CF YoY", "Free CF QoQ", "Free CF YoY",
    ]

    final_html = html_template.format(
        timestamp=current_time,
        table=html_table,
        score_idx=cols.index("Total Score"),
        number_targets=json.dumps(col_indices(number_cols)),
        percent_targets=json.dumps(col_indices(percent_cols)),
    )
    
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

    max_req = int(os.environ.get("MAX_REQ_PER_MIN", "75"))
    dataset = build_dataset(api_key, max_req_per_min=max_req)

    results = []

    for i, (symbol, payload) in enumerate(dataset.items()):
        logger.info(f"Processing [{i+1}/{len(dataset)}]: {symbol}")
        try:
            overview = payload["overview"]
            quote = payload["quote"]
            rsi = payload["rsi"]
            bs = payload["balance_sheet"]
            cf = payload["cash_flow"]
            inc = payload["income_statement"]

            market_cap = safe_float(overview.get("MarketCapitalization"))

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

    logger.info(f"Scan complete. Total processed: {len(results)}.")

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