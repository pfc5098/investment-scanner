# Change Column Name Format to Snake Case

## Changes
Update `src/scanner.py` where the `row` dictionary is constructed in the `main` function (around line 144) to use `snake_case` keys:
- `Symbol` -> `symbol`
- `Price` -> `price`
- `Volume` -> `volume`
- `RSI (14)` -> `rsi_14`
- `Market Cap` -> `market_cap`
- `P/E Ratio` -> `pe_ratio`
- `EPS` -> `eps`
- `Sector` -> `sector`
- `Industry` -> `industry`
- `Last Updated` -> `last_updated`

## Verification
1. Temporarily modify `src/scanner.py` to hardcode the "Magnificent Seven" symbols (`['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA']`) instead of fetching all active listings.
2. Run the script locally to generate a local CSV.
3. Verify the headers in the generated CSV output match the new `snake_case` format.
4. Use the `google_web_search` tool to cross-check the returned numbers (e.g., Price, Volume, Market Cap) for a subset of the Mega-7 stocks against current market data.
5. Revert the temporary hardcoding of symbols back to the original dynamic list fetching.
