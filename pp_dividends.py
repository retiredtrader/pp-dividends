#!/usr/bin/env python3
"""
Portfolio Performance Dividend Import Generator
================================================
Reads a PP CSV transaction export (English, comma-delimited) with columns:
    Date, Type, Security, Shares, Quote, Amount, Fees, Taxes,
    Net Transaction Value, Account, Offset Account, Note, Source, ISIN, Symbol

Fetches dividend history via yfinance for every security ever held,
calculates shares held on each ex-dividend date per account, and writes
a PP-compatible import CSV with:
  - One "Dividend" row  (cash inflow linked to the security)
  - One "Removal" row   (cash outflow same day / same amount)
  …so the deposit account stays at zero but PP counts dividends in the
  portfolio total return calculation.

Usage:
    pip install pandas yfinance
    python pp_dividends.py --input All_transactions.csv --output divs.csv

Optional flags:
    --date-from 2020-01-01    Only include dividends on/after this date
    --date-to   2024-12-31    Only include dividends on/before this date
    --tax-rate  0.15          Flat withholding tax 0–1 applied to gross amount
                              (default 0 — edit per-security in output if needed)
    --debug                   Print verbose diagnostic info to stderr
"""

import argparse
import re
import sys
import warnings
from datetime import date, timedelta

import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Transaction types that change share counts
# ---------------------------------------------------------------------------
BUY_TYPES  = {"Buy", "Delivery (Inbound)"}
SELL_TYPES = {"Sell", "Delivery (Outbound)"}

# ---------------------------------------------------------------------------
# Symbols that look like custom/internal codes and are not Yahoo tickers.
# Any symbol matching these prefixes will be skipped gracefully.
# ---------------------------------------------------------------------------
NON_YAHOO_PREFIXES = ("FIDELITY-",)

# ---------------------------------------------------------------------------
# Optional per-security symbol overrides — use this if a symbol in your
# export doesn't work on Yahoo Finance (e.g. wrong exchange suffix).
# Maps Security name → Yahoo ticker string, or None to skip entirely.
# ---------------------------------------------------------------------------
TICKER_OVERRIDES: dict[str, str | None] = {
    # Bonds / gilts / pension funds that never pay Yahoo dividends
    "UNITED KINGDOM GILT 10/2050":              None,
    "UNITED KINGDOM GILT 06/2025":              None,
    "UNITED KINGDOM GILT 01/2046":              None,
    "UNITED KINGDOM GILT 01/2025":              None,
    "UNITED KINGDOM GILT 01/2024":              None,
    "UNITED KINGDOM GILT 07/2023":              None,
    "UKGOVT OF IDX/LKD SNR 22/03/2051 GBP (T51)": None,
    "UKGOVT OF IDX/LKD SNR 10/08/2041 GBP (T41)": None,
    "USA, Note 2.75 15aug2042 30Y":             None,
    "USA, Note 2.25 15nov2027 10Y":             None,
    "USA, Note 1.5 15aug2026 10Y":              None,
    "BlackRock ICS Euro Liquidity Fund Heritage Acc": None,
    "Friends Life BlackRock World ex-UK Equity Index - Pension": None,
    "Friends Life BlackRock US Equity Index Aquila HP - Pension": None,
    "Friends Life BlackRock European Equity Index Aquila HP - Pension": None,
    "Friends Life BlackRock Japanese Equity Index Aquila HP - Pension": None,
    "Aviva Pension My Future Growth Pen FP":    None,
    "Nest Sharia Pension Fund (HSBC Islamic Global Equity Index)": None,
    "HSBC Index Tracker Investment Funds - FTSE All-World Index Fund": None,
    "Vanguard S&P 500 Admiral Class Mutual Fund*": None,
}


def parse_args():
    p = argparse.ArgumentParser(
        description="Generate PP dividend import CSV from account transaction export")
    p.add_argument("--input",     required=True,  help="Path to PP transaction export CSV")
    p.add_argument("--output",    required=True,  help="Path for generated import CSV")
    p.add_argument("--date-from", default=None,   help="Include dividends on/after YYYY-MM-DD")
    p.add_argument("--date-to",   default=None,   help="Include dividends on/before YYYY-MM-DD")
    p.add_argument("--tax-rate",  type=float, default=0.0,
                   help="Flat withholding tax rate 0–1 (default 0)")
    p.add_argument("--debug",     action="store_true")
    return p.parse_args()


def log(msg, debug=False, force=False):
    if force or debug:
        print(msg, file=sys.stderr)


def parse_shares(val):
    if pd.isna(val):
        return 0.0
    try:
        return float(str(val).replace(",", ""))
    except ValueError:
        return 0.0


def infer_currency(account_name: str) -> str:
    """Extract currency from account name suffix, e.g. 'M IBKR (USD)' → 'USD'.
    Treats GBX/GBp as GBP (pence accounts settle in GBP cash accounts)."""
    m = re.search(r'\(([A-Z]{3,4})\)\s*$', str(account_name))
    if m:
        c = m.group(1)
        return "GBP" if c in ("GBX", "GBP") else c
    return "GBP"


def infer_cash_account(securities_account: str, df_all: pd.DataFrame) -> str:
    """
    Derive the cash account name for a securities account.

    Priority:
      1. An explicit offset account seen on any Buy/Sell/Delivery row for this account.
      2. Build it from the securities account name + a currency suffix derived from
         the Quote column of its transactions (e.g. 'USD 47.00' → USD).
      3. Fall back to account + ' (GBP)'.

    This replaces the old '(Cash)' fallback.
    """
    # Try to find an explicit offset account from *any* transaction type
    offsets = df_all.loc[
        (df_all["Account"] == securities_account) & df_all["Offset Account"].notna(),
        "Offset Account"
    ].str.strip()
    offsets = offsets[offsets != ""]
    if not offsets.empty:
        return offsets.iloc[0]

    # Infer currency from Quote column values for this account
    quotes = df_all.loc[df_all["Account"] == securities_account, "Quote"].dropna()
    for q in quotes:
        q = str(q).strip()
        # Matches "USD 47.00", "GBX 674.40", "EUR 44.53" etc.
        m = re.match(r'^([A-Z]{2,4})\s', q)
        if m:
            ccy = m.group(1)
            # GBX stocks settle in GBP cash accounts
            if ccy == "GBX":
                ccy = "GBP"
            return f"{securities_account} ({ccy})"

    return f"{securities_account} (GBP)"


# ---------------------------------------------------------------------------
# FX helper
# ---------------------------------------------------------------------------
_fx_cache: dict = {}

def get_fx_rate(from_ccy: str, to_ccy: str, target_date, debug: bool) -> float:
    if from_ccy == to_ccy:
        return 1.0
    if from_ccy == "GBX" and to_ccy == "GBP":
        return 0.01
    if from_ccy == "GBP" and to_ccy == "GBX":
        return 100.0

    key = (from_ccy, to_ccy, str(target_date))
    if key in _fx_cache:
        return _fx_cache[key]

    td    = target_date if isinstance(target_date, date) else target_date.date()
    start = td - timedelta(days=7)
    end   = td + timedelta(days=3)

    def _fetch(pair):
        data = yf.download(pair, start=start, end=end,
                           progress=False, auto_adjust=True)
        if not data.empty:
            return float(data["Close"].iloc[-1])
        return None

    # Try direct pair first; if it involves GBX, also try the GBP bridge
    # (some pairs like USDGBX=X exist on Yahoo; the bridge is a safe fallback)
    pairs_to_try = [f"{from_ccy}{to_ccy}=X"]
    if to_ccy == "GBX":
        pairs_to_try.append(("GBP_BRIDGE", from_ccy))   # from→GBP ×100
    if from_ccy == "GBX":
        pairs_to_try.append(("GBX_BRIDGE", to_ccy))     # ÷100 then to_ccy

    for attempt in pairs_to_try:
        try:
            if attempt[0] == "GBP_BRIDGE":
                # fetch from_ccy→GBP, multiply by 100 to get from_ccy→GBX
                gbp_rate = _fetch(f"{attempt[1]}GBP=X")
                if gbp_rate is not None:
                    rate = gbp_rate * 100.0
                    log(f"    FX {from_ccy}→{to_ccy} on {td}: {rate:.6f} (via GBP bridge)", debug)
                    _fx_cache[key] = rate
                    return rate
            elif attempt[0] == "GBX_BRIDGE":
                # fetch GBP→to_ccy, divide by 100 for GBX→to_ccy
                gbp_rate = _fetch(f"GBP{attempt[1]}=X")
                if gbp_rate is not None:
                    rate = gbp_rate / 100.0
                    log(f"    FX {from_ccy}→{to_ccy} on {td}: {rate:.6f} (via GBP bridge)", debug)
                    _fx_cache[key] = rate
                    return rate
            else:
                rate = _fetch(attempt)
                if rate is not None:
                    log(f"    FX {from_ccy}→{to_ccy} on {td}: {rate:.6f}", debug)
                    _fx_cache[key] = rate
                    return rate
        except Exception as e:
            log(f"    FX lookup failed {attempt}: {e}", force=True)

    log(f"    FX {from_ccy}→{to_ccy}: all lookups failed, using 1.0", force=True)
    _fx_cache[key] = 1.0
    return 1.0


# ---------------------------------------------------------------------------
# Holdings timeline
# ---------------------------------------------------------------------------

def build_holdings(df: pd.DataFrame, debug: bool) -> dict:
    """
    Returns dict keyed by (securities_account, security_name):
        { "dates": [...], "shares": [...], "cash_account": str,
          "isin": str, "symbol": str }

    Handles:
      - Buys / Delivery (Inbound)  → add shares
      - Sells / Delivery (Outbound) → subtract shares
      - Rows with NaT dates are skipped with a warning; if a sell has NaT,
        a synthetic zero-out entry is appended at the END of the timeline so
        that the position is closed even if we don't know exactly when.
    """
    all_txns = df[
        df["Type"].isin(BUY_TYPES | SELL_TYPES) & df["Security"].notna()
    ].copy()

    # Separate rows with valid vs missing dates
    valid   = all_txns[all_txns["Date"].notna()].sort_values("Date")
    invalid = all_txns[all_txns["Date"].isna()]

    if not invalid.empty:
        for _, row in invalid.iterrows():
            msg = (f"  ⚠  NaT date on {row['Type']} of {row['Security']} "
                   f"in {row['Account']} — will zero-out position after all other txns.")
            log(msg, force=True)

    holdings: dict = {}
    cash_map: dict = {}   # securities_account → cash_account

    def _process(row, shares_delta_override=None):
        sec     = str(row["Security"]).strip()
        account = str(row["Account"]).strip()
        offset  = row.get("Offset Account", "")
        key     = (account, sec)
        shares  = parse_shares(row["Shares"]) if shares_delta_override is None else 0.0

        if pd.notna(offset) and str(offset).strip():
            cash_map[account] = str(offset).strip()

        if key not in holdings:
            holdings[key] = {
                "dates":        [],
                "shares":       [],
                "cash_account": None,
                "isin":         "",
                "symbol":       "",
                "quote_ccy":    "",
            }

        h = holdings[key]

        # Always update ISIN/symbol/quote_ccy from any row that has better data.
        # Priority: take the first non-blank value seen, EXCEPT for ISIN where
        # we prefer a real ISIN (starting with a letter) over a blank, and later
        # rows may supply the real one (e.g. user adds custom XX-prefix ISINs).
        raw_quote = str(row.get("Quote", "") or "").strip()
        _sym_raw  = row.get("Symbol", "")
        sym_here  = str(_sym_raw).strip() if pd.notna(_sym_raw) and str(_sym_raw).strip() not in ("", "nan") else ""
        _isin_raw = row.get("ISIN", "")
        isin_here = str(_isin_raw).strip() if pd.notna(_isin_raw) and str(_isin_raw).strip() not in ("", "nan") else ""

        if isin_here and not h["isin"]:
            h["isin"] = isin_here
        # Prefer a later ISIN that looks "more real" (e.g. XX custom > blank,
        # but a genuine 2-letter country code > XX custom)
        if isin_here and h["isin"] and isin_here != h["isin"]:
            # Keep whichever starts with a recognised country-like prefix;
            # real ISINs start with 2 uppercase letters that are not "XX"
            def _isin_rank(s):
                if not s:             return 0
                if s.startswith("XX"): return 1
                return 2
            if _isin_rank(isin_here) > _isin_rank(h["isin"]):
                h["isin"] = isin_here

        if sym_here and not h["symbol"]:
            h["symbol"] = sym_here

        # Derive quote_ccy from this row's Quote column:
        #   - Explicit prefix "GBX ..."  → GBX  (pence-quoted)
        #   - Explicit prefix "EUR ..."  → EUR
        #   - Plain number (no prefix)   → GBP if .L ticker, else from suffix
        # We update quote_ccy whenever we get a more explicit value.
        m_ccy = re.match(r'^([A-Z]{2,4})\s', raw_quote)
        if m_ccy:
            row_ccy = m_ccy.group(1)
        elif raw_quote and sym_here.upper().endswith(".L"):
            # Plain numeric quote on an LSE stock means GBP (whole pounds),
            # NOT GBX. GBX stocks always have an explicit "GBX" prefix.
            row_ccy = "GBP"
        elif raw_quote and sym_here:
            row_ccy = _currency_from_ticker(sym_here)
        else:
            row_ccy = ""

        if row_ccy:
            # Buy/Sell quotes are authoritative — they reflect how the broker
            # actually trades the security.  Delivery (Inbound/Outbound) quotes
            # may reflect a different share class or FX valuation (e.g. IBKR
            # recording VGEU.DE in EUR while the same security trades as VEUR.L
            # in GBP).  So: set quote_ccy freely from any row when blank, but
            # only allow a Delivery row to set it if no Buy/Sell has done so.
            is_trade = row["Type"] in (BUY_TYPES - {"Delivery (Inbound)"}) | (SELL_TYPES - {"Delivery (Outbound)"})
            if not h["quote_ccy"]:
                h["quote_ccy"] = row_ccy
                h["_quote_ccy_from_trade"] = is_trade
            elif is_trade and not h.get("_quote_ccy_from_trade"):
                # A real trade overrides a previously delivery-inferred value
                h["quote_ccy"] = row_ccy
                h["_quote_ccy_from_trade"] = True

        if shares_delta_override == "zero":
            # Close-out: record 0 shares at a sentinel "far future" date
            holdings[key]["dates"].append(date(9999, 12, 31))
            holdings[key]["shares"].append(0.0)
            return

        delta = shares if row["Type"] in BUY_TYPES else -shares
        prev  = holdings[key]["shares"][-1] if holdings[key]["shares"] else 0.0
        holdings[key]["dates"].append(row["Date"].date())
        holdings[key]["shares"].append(max(prev + delta, 0.0))

    # Process valid-date rows in chronological order
    for _, row in valid.iterrows():
        _process(row)

    # For NaT sells/deliveries-outbound: zero out the position at end of timeline
    for _, row in invalid.iterrows():
        if row["Type"] in SELL_TYPES:
            _process(row, shares_delta_override="zero")
        # NaT buys are ignored (we can't know when they happened)

    for (account, _), h in holdings.items():
        # Store both the explicit cash account (if any) and the bare account name.
        # build_rows will pick the right currency-specific cash account per dividend.
        h["cash_account"]      = cash_map.get(account)   # e.g. "IBKR account (GBP)"
        h["cash_account_base"] = account                 # e.g. "IBKR account"

    log(f"  Built {len(holdings)} (account, security) holding records.", debug)
    return holdings


def shares_on_date(holding: dict, target_date: date) -> float:
    qty = 0.0
    for d, s in zip(holding["dates"], holding["shares"]):
        if d <= target_date:
            qty = s
        else:
            break
    return max(qty, 0.0)


# ---------------------------------------------------------------------------
# Ticker resolution
# ---------------------------------------------------------------------------

def resolve_ticker(security: str, isin: str, symbol: str, debug: bool) -> str | None:
    """
    Priority:
      1. TICKER_OVERRIDES (explicit skip = None, or explicit remap)
      2. Symbol from CSV (unless it's a custom non-Yahoo code)
      3. ISIN direct (works for many US securities on Yahoo)
    """
    if security in TICKER_OVERRIDES:
        t = TICKER_OVERRIDES[security]
        if t is None:
            log(f"  ○ Skip (override=None): {security}", force=True)
        return t

    if symbol and not any(symbol.startswith(p) for p in NON_YAHOO_PREFIXES):
        return symbol

    if isin:
        return isin   # Yahoo accepts many ISINs directly

    log(f"  ? No ticker resolvable for: {security}", force=True)
    return None


# ---------------------------------------------------------------------------
# Dividend fetching
# ---------------------------------------------------------------------------

# Maps Yahoo Finance exchange suffix → currency.
# GBX (pence) is handled downstream; .L stocks trade in GBp so we put GBX here
# so the ÷100 conversion fires correctly.
_SUFFIX_CCY: dict[str, str] = {
    ".L":   "GBX",   # London Stock Exchange (pence)
    ".IL":  "GBX",   # London (international order book, also pence)
    ".PA":  "EUR",   # Euronext Paris
    ".DE":  "EUR",   # XETRA / Frankfurt
    ".AS":  "EUR",   # Euronext Amsterdam
    ".BR":  "EUR",   # Euronext Brussels
    ".MI":  "EUR",   # Borsa Italiana
    ".MC":  "EUR",   # BME (Madrid)
    ".LS":  "EUR",   # Euronext Lisbon
    ".HE":  "EUR",   # Nasdaq Helsinki
    ".ST":  "SEK",   # Nasdaq Stockholm
    ".OL":  "NOK",   # Oslo Børs
    ".CO":  "DKK",   # Nasdaq Copenhagen
    ".TO":  "CAD",   # Toronto Stock Exchange
    ".AX":  "AUD",   # ASX
    ".NZ":  "NZD",   # NZX
    ".HK":  "HKD",   # Hong Kong
    ".T":   "JPY",   # Tokyo
    ".SS":  "CNY",   # Shanghai
    ".SZ":  "CNY",   # Shenzhen
    ".BO":  "INR",   # BSE India
    ".NS":  "INR",   # NSE India
    ".SW":  "CHF",   # SIX Swiss Exchange
    ".VI":  "EUR",   # Vienna
}

def _currency_from_ticker(ticker: str) -> str:
    """
    Infer the traded currency from the Yahoo Finance ticker exchange suffix.
    Tickers with no dot suffix (e.g. SPY, T, VTI) are USD-listed.
    """
    ticker = ticker.upper()
    # Longest-match first so '.IL' beats '.L' etc.
    for suffix in sorted(_SUFFIX_CCY, key=len, reverse=True):
        if ticker.endswith(suffix.upper()):
            return _SUFFIX_CCY[suffix]
    return "USD"   # no suffix → US-listed


def fetch_dividends(ticker: str, date_from, date_to, debug: bool) -> pd.DataFrame:
    """Returns DataFrame [date, dps, currency] or empty."""
    try:
        t    = yf.Ticker(ticker)
        divs = t.dividends
        if divs is None or divs.empty:
            return pd.DataFrame(columns=["date", "dps", "currency"])

        df = divs.reset_index()
        df.columns = ["date", "dps"]
        df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).dt.normalize()

        if date_from:
            df = df[df["date"] >= pd.to_datetime(date_from)]
        if date_to:
            df = df[df["date"] <= pd.to_datetime(date_to)]

        if df.empty:
            return pd.DataFrame(columns=["date", "dps", "currency"])

        try:
            currency = t.fast_info.currency or None
        except Exception:
            currency = None

        if not currency:
            # fast_info.currency failed — infer from ticker exchange suffix
            currency = _currency_from_ticker(ticker)

        # Yahoo Finance returns dividends for LSE stocks in pence (GBp / GBx).
        # Normalise to our internal 'GBX' token so get_fx_rate applies ÷100.
        if currency in ("GBp", "GBx", "GBX"):
            currency = "GBX"
        df["currency"] = currency

        log(f"    {ticker}: {len(df)} div events, ccy={currency}", debug)
        return df

    except Exception as e:
        log(f"    ERROR fetching {ticker}: {e}", force=True)
        return pd.DataFrame(columns=["date", "dps", "currency"])


# ---------------------------------------------------------------------------
# Build output rows
# ---------------------------------------------------------------------------

def build_rows(security: str, account: str, holding: dict,
               dividends_df: pd.DataFrame, tax_rate: float,
               debug: bool) -> list[dict]:
    rows            = []
    isin            = holding["isin"]
    symbol          = holding["symbol"]
    cash_acct_base  = holding.get("cash_account_base") or holding["cash_account"]

    for _, drow in dividends_df.iterrows():
        ex_date = drow["date"].date()
        dps     = float(drow["dps"])
        div_ccy = drow["currency"]   # already normalised: GBX, GBP, USD, EUR …
        shares  = shares_on_date(holding, ex_date)

        if shares <= 0:
            continue

        # Settle in the security's native quote currency so PP sees a
        # single-currency transaction and needs no exchange rate.
        # e.g. GBX stock + GBX dividend  → no conversion needed
        #      GBX stock + USD dividend  → convert USD → GBX via FX
        #      USD stock + USD dividend  → no conversion needed
        quote_ccy = holding.get("quote_ccy", "") or div_ccy
        if div_ccy == quote_ccy:
            settle_ccy   = div_ccy
            gross_settle = round(dps * shares, 2)
            fx_rate      = None   # same currency, no exchange rate needed
        else:
            # Cross-currency: fetch rate and convert into the security's currency
            fx_rate      = get_fx_rate(div_ccy, quote_ccy, ex_date, debug)
            settle_ccy   = quote_ccy
            gross_settle = round(dps * shares * fx_rate, 2)

        taxes = round(gross_settle * tax_rate, 2)
        net   = round(gross_settle - taxes, 2)

        # Build the cash account name from the base + settlement currency.
        # e.g. "IBKR account" + GBX → "IBKR account (GBX)"
        cash_account = f"{cash_acct_base} ({settle_ccy})"

        date_str = drow["date"].strftime("%Y-%m-%dT%H:%M")

        if fx_rate is not None:
            note = (
                f"Dividend {ex_date} | {shares:g} shares "
                f"x {dps:.6g} {div_ccy} -> {gross_settle:.2f} {settle_ccy}"
                f" (FX {fx_rate:.4f})"
                + (f" | Tax {taxes:.2f} {settle_ccy}" if taxes else "")
            )
        else:
            note = (
                f"Dividend {ex_date} | {shares:g} shares "
                f"x {dps:.6g} {settle_ccy} = {gross_settle:.2f} {settle_ccy}"
                + (f" | Tax {taxes:.2f} {settle_ccy}" if taxes else "")
            )

        rows.append({
            "date":         date_str,
            "security":     security,
            "isin":         isin,
            "symbol":       symbol,
            "shares":       shares,
            "gross":        gross_settle,
            "taxes":        taxes,
            "net":          net,
            "currency":     settle_ccy,
            "sec_account":  account,
            "cash_account": cash_account,
            "note":         note,
            "quote_ccy":    holding.get("quote_ccy", ""),
            "exchange_rate": fx_rate,   # None = same currency, float = cross-currency FX
        })

    return rows


# ---------------------------------------------------------------------------
# Write PP import CSV
# ---------------------------------------------------------------------------

PP_HEADERS = [
    "Date", "Type", "Value", "Transaction Currency",
    "Gross Amount", "Currency Gross Amount",
    "Exchange Rate", "Fees", "Taxes",
    "Shares", "ISIN", "Ticker Symbol", "Security Name",
    "Note", "Cash Account", "Securities Account", "Offset Account",
]


def to_pp_df(all_rows: list[dict]) -> pd.DataFrame:
    out = []
    for r in all_rows:
        # Identify the security by ISIN + full exchange-qualified ticker
        # (e.g. VJPN.L, VDJP.L, VGEU.DE).  The full suffix makes each
        # ticker unique in PP so the "Ticker exists multiple times" error
        # cannot recur (that error arose only when the bare root matched
        # two securities; VJPN.L vs VJPN.DE are unambiguous).
        # Dividend row — credits cash account, linked to security
        out.append({
            "Date":                     r["date"],
            "Type":                     "Dividend",
            "Value":                    r["net"],
            "Transaction Currency":     r["currency"],
            "Gross Amount":             r["gross"],
            "Currency Gross Amount":    r["currency"],
            "Exchange Rate":            r.get("exchange_rate") or "",
            "Fees":                     "",
            "Taxes":                    r["taxes"] if r["taxes"] else "",
            "Shares":                   r["shares"],
            "ISIN":                     r["isin"],
            "Ticker Symbol":            r["symbol"],
            # Omit Security Name when no ISIN is set: with a blank ISIN, PP
            # matches on ticker alone — including the name risks hitting a
            # same-named security with the wrong currency (e.g. two share
            # classes of the same fund under one name, differentiated only
            # by ticker).  When an ISIN is present it anchors the match so
            # the name is safe to include as a display hint.
            "Security Name":            r["security"] if r["isin"] else "",
            "Note":                     r["note"],
            "Cash Account":             r["cash_account"],
            "Securities Account":       r["sec_account"],
            "Offset Account":           "",
        })
        # Removal row — debits cash account same day, net balance = 0
        out.append({
            "Date":                     r["date"],
            "Type":                     "Removal",
            "Value":                    r["net"],
            "Transaction Currency":     r["currency"],
            "Gross Amount":             "",
            "Currency Gross Amount":    "",
            "Exchange Rate":            "",
            "Fees":                     "",
            "Taxes":                    "",
            "Shares":                   "",
            "ISIN":                     "",
            "Ticker Symbol":            "",
            "Security Name":            "",
            "Note":                     "Auto-removal: " + r["note"],
            "Cash Account":             r["cash_account"],
            "Securities Account":       "",
            "Offset Account":           "",
        })
    return pd.DataFrame(out, columns=PP_HEADERS)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args  = parse_args()
    debug = args.debug

    print("\n=== Portfolio Performance Dividend Import Generator ===\n")

    # 1. Load
    print("[1/5] Loading transaction export...")
    df = pd.read_csv(args.input, dtype=str, encoding="utf-8-sig")
    df.columns = df.columns.str.strip()
    df["Date"] = pd.to_datetime(df["Date"], dayfirst=False, errors="coerce")  # CSV is always YYYY-MM-DD
    for col in ("ISIN", "Symbol"):
        if col not in df.columns:
            df[col] = ""
        else:
            df[col] = df[col].fillna("").str.strip()

    # Keep full df (inc. NaT rows) for holdings so NaT sells close positions.
    df_all = df.copy()
    df     = df.dropna(subset=["Date"])

    print(f"      {len(df)} rows | {df['Account'].nunique()} accounts | "
          f"{df['Date'].min().date()} → {df['Date'].max().date()}\n")

    # 2. Build holdings (pass df_all so NaT-dated sells are honoured)
    print("[2/5] Building share-holdings timeline...")
    holdings = build_holdings(df_all, debug)

    # Resolve cash accounts: explicit offset where known, else infer from transactions.
    # For multi-currency accounts this is just a fallback; build_rows uses
    # cash_account_base + div currency to pick the right deposit account per dividend.
    for (account, _), h in holdings.items():
        if not h["cash_account"]:
            h["cash_account"] = infer_cash_account(account, df_all)
        # Derive base name: strip any trailing " (CCY)" suffix
        h["cash_account_base"] = re.sub(r'\s*\([A-Z]{2,4}\)\s*$', '', h["cash_account"])

    active   = {k: v for k, v in holdings.items()
                if any(s > 0 for s in v["shares"])}
    print(f"      {len(active)} (account, security) positions tracked.\n")

    # 3. Fetch dividends — one API call per unique ticker symbol.
    # We key the cache by ticker (not security name) because the same security
    # name can appear with different tickers/quote currencies across accounts
    # (e.g. "Vanguard FTSE Japan UCITS ETF" as VDJP.L and VJPN.DE).
    print("[3/5] Fetching dividend history from Yahoo Finance...")

    div_cache:    dict[str, pd.DataFrame] = {}   # ticker → dividends df
    skipped_explicit: list[str] = []
    skipped_no_divs:  list[str] = []

    for (account, sec), holding in active.items():
        isin   = holding["isin"]
        symbol = holding["symbol"]
        ticker = resolve_ticker(sec, isin, symbol, debug)
        holding["_ticker"] = ticker   # stash for use in step 4

        if ticker is None:
            if sec not in [s for s in skipped_explicit]:
                skipped_explicit.append(sec)
            if ticker not in div_cache:
                div_cache[ticker or sec] = pd.DataFrame(columns=["date", "dps", "currency"])
            continue

        if ticker not in div_cache:
            print(f"  Fetching  {ticker:<14}  {sec[:60]}")
            divs = fetch_dividends(ticker, args.date_from, args.date_to, debug)
            div_cache[ticker] = divs
            if divs.empty:
                skipped_no_divs.append(ticker)

    # 4. Calculate per-account dividend rows
    print(f"\n[4/5] Calculating per-account dividend amounts...")
    all_rows: list[dict] = []

    for (account, sec), holding in sorted(active.items()):
        ticker = holding.get("_ticker")
        divs   = div_cache.get(ticker or sec, pd.DataFrame(columns=["date", "dps", "currency"]))
        if divs.empty:
            continue
        rows = build_rows(sec, account, holding, divs, args.tax_rate, debug)
        if rows:
            print(f"  ✓  {account:<30}  {sec[:50]:<50}  {len(rows)} events")
        all_rows.extend(rows)

    print(f"\n      Dividend events : {len(all_rows)}")
    print(f"      Output rows     : {len(all_rows) * 2}  "
          f"({len(all_rows)} Dividends + {len(all_rows)} Removals)")
    if skipped_explicit:
        print(f"      Skipped (bonds/funds/no ticker): {len(skipped_explicit)}")
    if skipped_no_divs:
        print(f"      Skipped (no dividend history) : {len(skipped_no_divs)}")
        if debug:
            for s in skipped_no_divs:
                print(f"        - {s}")

    if not all_rows:
        print("\nNo dividend rows generated. Check --date-from/--date-to or add overrides.")
        sys.exit(0)

    # 5. Write output
    print(f"\n[5/5] Writing → {args.output}")
    out_df = to_pp_df(all_rows)
    out_df = out_df.sort_values(
        ["Date", "Securities Account", "Security Name", "Type"],
        na_position="last"
    )
    out_df.to_csv(args.output, index=False, encoding="utf-8-sig")

    print(f"\n✓  Done!  {args.output}  ({len(out_df)} rows)\n")
    print("""How to import into Portfolio Performance:
  1. File → Import → CSV Files → select the output file
  2. PP should auto-detect all columns (English headers)
  3. Review the preview:
       Dividend rows  → credit the security's linked cash account
       Removal rows   → debit the same amount on the same date
     Net effect: deposit account stays at zero; dividends appear in
     total-return / performance charts for each securities account.
  4. If any amounts look wrong, check the FX conversion in the Note field.
""")


if __name__ == "__main__":
    main()
