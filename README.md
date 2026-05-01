# PP Dividend Import Generator

Automatically backfills dividend history into [Portfolio Performance](https://www.portfolio-performance.info/) from a single transaction export CSV, without manual data entry.

## What it does

Portfolio Performance does not fetch dividend history automatically. This script bridges that gap:

1. Reads your full PP transaction export (all accounts, all securities).
2. Reconstructs exactly how many shares you held in each account on every historical ex-dividend date.
3. Fetches dividend-per-share data from Yahoo Finance for every security ever held.
4. Writes a PP-compatible import CSV containing, for each dividend event:
   - A **Dividend** transaction — credits the correct cash deposit account, linked to the security.
   - A **Removal** (Outbound Delivery of cash) on the same day for the same amount — so the deposit account balance stays at zero, but PP correctly counts the dividend in total return calculations.

The note field on each row records the full calculation detail, e.g.:
```
Dividend 2024-03-15 | 1005 shares × 21.929p = 220.39 GBP
```

---

## Requirements

- Python 3.10+
- `pandas`
- `yfinance`

Install dependencies:
```bash
pip install pandas yfinance
```

---

## Usage

### 1. Export your transactions from Portfolio Performance

In PP: **File → Export → CSV (Portfolio Transactions / Account Transactions)**

Export **all accounts** into a single file. The script expects English-language column headers in this format:

```
Date, Type, Security, Shares, Quote, Amount, Fees, Taxes,
Net Transaction Value, Account, Offset Account, Note, Source, ISIN, Symbol
```

Dates must be in `YYYY-MM-DD` format (PP's default English export).

### 2. Run the script

```bash
python pp_dividends.py --input All_transactions.csv --output dividends_import.csv
```

### 3. Import into Portfolio Performance

In PP: **File → Import → CSV (Portfolio Transactions / Account Transactions)**

Select `dividends_import.csv`. Map columns if prompted (headers should auto-match). Review the preview, then confirm.

---

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--input` | *(required)* | Path to your PP transaction export CSV |
| `--output` | *(required)* | Path to write the generated import CSV |
| `--date-from` | none | Only include dividends on or after `YYYY-MM-DD` |
| `--date-to` | none | Only include dividends on or before `YYYY-MM-DD` |
| `--tax-rate` | `0` | Flat withholding tax rate as a decimal, e.g. `0.15` for 15%. Applied uniformly — edit individual rows in the output if you need per-security rates. |
| `--debug` | off | Print verbose diagnostics to stderr (holdings timeline, FX lookups, skipped tickers) |

### Example with options

```bash
python pp_dividends.py \
  --input All_transactions.csv \
  --output dividends_import.csv \
  --date-from 2018-01-01 \
  --tax-rate 0.15 \
  --debug
```

---

## How holdings are calculated

The script builds a chronological timeline of share counts per `(account, security)` pair:

- **Buys** and **Delivery (Inbound)** increase the position.
- **Sells** and **Delivery (Outbound)** decrease it (never below zero).
- Transactions with unparseable dates are flagged with a warning; sell-side rows in this state zero out the position at the end of the timeline as a safety measure.

For each ex-dividend date, the script looks up how many shares were held *on that date* (i.e. the most recent transaction at or before the ex-date). If the position was zero — because the stock had already been sold — no dividend row is generated.

> **Important:** PP must export dates in `YYYY-MM-DD` format. If you see unexpected holdings behaviour, run with `--debug` and check the timeline printed for the affected security.

---

## Currency handling

### LSE-listed securities (GBX / pence)

Yahoo Finance reports dividends for London Stock Exchange securities in pence (`GBp`). The script converts these to GBP automatically (÷ 100) and routes them to the `(GBP)` deposit account. The note shows the calculation in pence for transparency, e.g.:

```
Dividend 2024-03-15 | 1005 shares × 21.929p = 220.39 GBP
```

### Multi-currency brokerage accounts (e.g. IBKR)

Interactive Brokers and similar multi-currency accounts hold separate deposit sub-accounts per currency (e.g. `IBKR account (GBP)`, `IBKR account (USD)`). The script detects the dividend currency for each event and routes it to the matching sub-account automatically — no manual mapping needed.

### Known quirk: USD/EUR dividends on LSE-listed Vanguard ETFs at IBKR

Some LSE-listed Vanguard ETFs pay dividends in their underlying market currency rather than GBP when held at IBKR:

| Fund | Ticker | Dividend currency at IBKR |
|------|--------|--------------------------|
| Vanguard FTSE Developed Asia Pacific ex Japan UCITS ETF | `VDPX.L` / `VAPX.L` | USD |
| Vanguard FTSE Japan UCITS ETF | `VJPN.L` | USD |
| Vanguard FTSE Developed Europe UCITS ETF | `VEUR.L` / `VGEU.DE` | EUR |

UK retail brokers (Hargreaves Lansdown, iWeb, etc.) auto-convert these to GBP before crediting, so the issue doesn't arise there. At IBKR the script will record these dividends in USD or EUR and route them to the corresponding deposit sub-account. The **amount is correct** — only the currency label differs from what a UK broker would show. Since the Removal transaction withdraws the same amount on the same day, the deposit account balance remains zero regardless.

### Currency inference priority

For each dividend event the script determines settlement currency in this order:

1. `fast_info.currency` from Yahoo Finance (most accurate when available).
2. Exchange suffix of the ticker symbol (`.L` → GBP, `.DE`/`.PA`/`.AS` → EUR, `.TO` → CAD, `.AX` → AUD, no suffix → USD, etc.).
3. Falls back to GBP.

---

## Skipping securities

Some securities in a PP export will never have Yahoo Finance data — gilts, pension fund units, liquidity funds, delisted stocks, etc. These are listed in the `TICKER_OVERRIDES` dict near the top of the script with a value of `None`:

```python
TICKER_OVERRIDES: dict[str, str | None] = {
    "UNITED KINGDOM GILT 10/2050": None,
    "Aviva Pension My Future Growth Pen FP": None,
    # …
}
```

Add any security name from your export here to suppress the Yahoo Finance lookup and any associated warnings. You can also use this dict to fix incorrect tickers:

```python
TICKER_OVERRIDES = {
    "Berkshire Hathaway B": "BRK-B",   # override wrong symbol
    "My Internal Fund":     None,       # skip entirely
}
```

---

## Cash account naming

The script determines deposit account names by examining the `Offset Account` column in your export:

- If a buy or sell for a given securities account has an explicit offset account (e.g. `Securities Account Name (GBP)`), the base name (`Securities Account Name`) is extracted and the dividend currency is appended for each event.
- If no offset account is recorded (common for `Delivery (Inbound)` transfers), the currency is inferred from the `Quote` column of transactions in that account.
- Last resort: `Securities Account Name (GBP)`.

The result is always `Account Name (CCY)` — never the `(Cash)` placeholder that PP sometimes uses internally.

---

## Output format

The generated CSV has these columns, matching PP's import schema:

```
Date, Type, Value, Transaction Currency, Gross Amount, Currency Gross Amount,
Exchange Rate, Fees, Taxes, Shares, ISIN, Ticker Symbol, Security Name,
Note, Cash Account, Securities Account, Offset Account
```

Each dividend event produces **two rows**: a `Dividend` and a `Removal`. Do not delete the Removal rows — they are intentional.

---

## Limitations

- **Data quality depends on Yahoo Finance.** Some tickers have incomplete or incorrect dividend history. Always cross-check a sample against official sources (company investor relations pages, broker statements) before relying on the output.
- **No withholding tax per security.** The `--tax-rate` flag applies a single flat rate. If you need different rates per country or security, apply them manually to the output CSV before importing.
- **Ex-date vs pay-date.** Yahoo Finance reports ex-dividend dates. PP records the transaction on the ex-date, not the payment date. This is standard practice for return calculations but may differ from your broker statement dates.
- **Spin-offs and special dividends** may appear in Yahoo's dividend history and inflate totals. Review the output for unusually large one-off payments and remove them if appropriate.

---

## Troubleshooting

**"No dividend data found" for a ticker I expect to have dividends**
Run with `--debug`. Check whether Yahoo Finance recognises the ticker symbol from your PP export. Use `TICKER_OVERRIDES` to supply the correct Yahoo ticker if needed.

**Dividends showing after a stock was sold**
Run with `--debug` to print the holdings timeline for that security. Verify the sell transaction has a valid `YYYY-MM-DD` date in your PP export. Transactions with unparseable dates are flagged in the debug output.

**Amounts look 100× too large for an LSE stock**
The security's ticker may not be resolving to the `.L` suffix correctly. Check that the `Symbol` column in your PP export contains the full Yahoo ticker (e.g. `BP.L`, not just `BP`). Add a mapping in `TICKER_OVERRIDES` if needed.

**Dividend routed to wrong currency sub-account**
If a security pays dividends in an unexpected currency (see the IBKR/Vanguard ETF quirk above), the cash account name will reflect that currency. You can leave it as-is (the net effect on total return is identical) or manually edit the relevant rows in the output CSV before importing.

---

## License

MIT
