

import os
import re
from pathlib import Path
from typing import List, Dict, Optional

import argparse
import pandas as pd
import pdfplumber

# -----------------------------
# Configuration
# -----------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR = SCRIPT_DIR / "input_pdfs"   # folder containing PDFs next to the script
OUTPUT_XLSX = SCRIPT_DIR / "Pledges_Output.xlsx"

# Defaults: set to True if desired
DEFAULT_PLEDGE_EQUALS_PAYMENT = True
DEFAULT_PERCENTAGE_100 = True

# Optional lookup file to backfill account numbers:
# Excel with two columns: fullName, ACCOUNTNUMBER
ACCOUNT_LOOKUP_XLSX = SCRIPT_DIR / "lookup_accounts.xlsx"  # set to None if not using

# Output columns 
TARGET_COLUMNS = [
    "Individuals.ACCOUNTNUMBER",
    "Individuals.fullName",
    "Individuals.Transactions.TOTALPLEDGEAMOUNT",
    "Individuals.Transactions.TOTALPAYMENTAMOUNT",
    "Individuals.Transactions.PAYMENTTYPE",
    "Individuals.Transactions.CHECKNUMBER",
    "Individuals.Transactions.DCDetails.BOOKLABEL",  # GN1–GN7
    "Individuals.Transactions.DCDetails.DESPERCENTAGE",
    "Source File",
    "Seq",
]

# -----------------------------
# Regex patterns
# -----------------------------
# Example line: 5250031143286 JAMES ROBERT BOYD 2727 Check 100.00 4600055
# Make Name sturdy: it ends right before CheckNumber (a numeric token).
LINE_PATTERN = re.compile(r"""
    (?P<Seq>\d{13})\s+                       # Seq (13 digits)
    (?P<Name>.*?)\s+                         # Name (non-greedy up to next numeric token)
    (?P<CheckNumber>\d{1,10})\s+             # Check number
    (?P<PaymentType>Check|Cash|Card|ACH)\s+  # Payment type
    (?P<Amount>\d+(?:\.\d{2}))\s+            # Amount
    (?P<BatchNumber>\d+)                     # Batch number (ignored after capture)
""", re.VERBOSE)

# GN label can appear as "GN1", "GN-2", "GN 3"
GN_PATTERN = re.compile(r"""
    \bGN\s*[- ]?(?P<gn>[1-7])\b
""", re.IGNORECASE | re.VERBOSE)


# -----------------------------
# Helpers
# -----------------------------
def detect_gn_label_on_page(text: str) -> Optional[str]:
    """
    Detect GN label (GN1–GN7) from a text snippet (line or nearby context).
    Returns 'GN#' or None.
    """
    m = GN_PATTERN.search(text or "")
    return f"GN{m.group('gn')}" if m else None


def extract_rows_from_pdf(pdf_path: Path, debug: bool = False) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            # Get text; if your PDFs are scans, text may be empty (OCR needed).
            text = page.extract_text() or ""

            # Process the page line-by-line so GN labels only apply to the same line.
            lines = text.splitlines()

            matched_any = False
            for ln_num, line in enumerate(lines, start=1):
                # Normalize whitespace per line
                line_flat = re.sub(r"\s+", " ", line).strip()
                for m in LINE_PATTERN.finditer(line_flat):
                    matched_any = True
                    seq = m.group("Seq").strip()
                    name = m.group("Name").strip()
                    check_no = m.group("CheckNumber").strip()
                    pay_type = m.group("PaymentType").strip()
                    amt_str = m.group("Amount").strip()

                    payment = float(amt_str)
                    pledge = payment if DEFAULT_PLEDGE_EQUALS_PAYMENT else None
                    percent = 100 if DEFAULT_PERCENTAGE_100 else None

                    # Detect GN only within the same line (use detector result, default to GN1)
                    line_GNF_label = detect_gn_label_on_page(line_flat) or "GN1"

                    row = {
                        "Individuals.ACCOUNTNUMBER": "",  # filled via lookup if provided
                        "Individuals.fullName": name,
                        "Individuals.Transactions.TOTALPLEDGEAMOUNT": pledge,
                        "Individuals.Transactions.TOTALPAYMENTAMOUNT": payment,
                        "Individuals.Transactions.PAYMENTTYPE": pay_type,
                        "Individuals.Transactions.CHECKNUMBER": check_no,
                        "Individuals.Transactions.DCDetails.BOOKLABEL": line_GNF_label,
                        "Individuals.Transactions.DCDetails.DESPERCENTAGE": percent,
                        "Source File": pdf_path.name,
                        "Seq": seq,
                    }
                    rows.append(row)
            if not matched_any:
                print(f"[WARN] No transaction lines matched on {pdf_path.name} (page {page_num}).")
                if debug:
                    # Show candidate snippets around amounts and 13-digit seqs to help debug
                    print("[DEBUG] Page text snippet (first 300 chars):")
                    page_text_flat = re.sub(r"\s+", " ", text).strip()
                    print(page_text_flat[:300])
                    amount_pat = re.compile(r"\d+\.\d{2}")
                    seq13_pat = re.compile(r"\d{13}")
                    amounts = list(amount_pat.finditer(page_text_flat))
                    seqs = list(seq13_pat.finditer(page_text_flat))
                    print(f"[DEBUG] Found {len(amounts)} amount-like tokens and {len(seqs)} 13-digit sequences on page {page_num}.")
                    if amounts:
                        print("[DEBUG] Amount contexts:")
                        for a in amounts[:10]:
                            start = max(0, a.start() - 50)
                            end = min(len(page_text_flat), a.end() + 50)
                            print(f"...{page_text_flat[start:end]}...")
                    if seqs:
                        print("[DEBUG] 13-digit seqs (first 10):")
                        for s in seqs[:10]:
                            start = max(0, s.start() - 20)
                            end = min(len(page_text_flat), s.end() + 20)
                            print(f"...{page_text_flat[start:end]}...")
    return rows


def load_account_lookup(path: Optional[Path]) -> Optional[pd.DataFrame]:
    if path and path.exists():
        df = pd.read_excel(path, engine="openpyxl")
        # Expect columns: fullName, ACCOUNTNUMBER
        df = df.rename(columns={c: c.strip() for c in df.columns})
        if not {"fullName", "ACCOUNTNUMBER"} <= set(df.columns):
            print("[WARN] Lookup file missing required columns: fullName, ACCOUNTNUMBER")
            return None
        # Normalize names to uppercase to match report style
        df["fullName_key"] = df["fullName"].astype(str).str.upper().str.strip()
        return df[["fullName_key", "ACCOUNTNUMBER"]].drop_duplicates()
    return None


def apply_account_lookup(df: pd.DataFrame, lookup_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if lookup_df is None:
        return df
    df["fullName_key"] = df["Individuals.fullName"].astype(str).str.upper().str.strip()
    df = df.merge(lookup_df, on="fullName_key", how="left")
    # Fill account numbers where found
    df["Individuals.ACCOUNTNUMBER"] = df["Individuals.ACCOUNTNUMBER"].mask(
        df["Individuals.ACCOUNTNUMBER"].eq(""),
        df["ACCOUNTNUMBER"]
    )
    df = df.drop(columns=["fullName_key", "ACCOUNTNUMBER"])
    return df


# -----------------------------
# Main
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Extract pledges from PDFs")
    parser.add_argument("--debug", action="store_true", help="Print debug information for unmatched pages")
    args = parser.parse_args()

    INPUT_DIR.mkdir(exist_ok=True)
    pdf_files = sorted(p for p in INPUT_DIR.glob("*.pdf"))

    if not pdf_files:
        print(f"[WARN] No PDFs found in {INPUT_DIR}.")
        print("       Place your files (e.g., 'GNEF LB 12-11-2025.pdf') inside the 'input_pdfs' folder next to the script.")
        return

    all_rows: List[Dict[str, str]] = []

    # Process every PDF in the input folder
    for pdf_path in pdf_files:
        rows = extract_rows_from_pdf(pdf_path, debug=args.debug)
        all_rows.extend(rows)
        print(f"[OK] {pdf_path.name}: extracted {len(rows)} rows")

    if not all_rows:
        print("[ERROR] No rows extracted from any PDF. Check regex and sample text.")
        return

    # Build DataFrame with your target columns
    df = pd.DataFrame([{c: r.get(c, "") for c in TARGET_COLUMNS} for r in all_rows])

    # Coerce numeric types where appropriate
    for col in [
        "Individuals.Transactions.TOTALPLEDGEAMOUNT",
        "Individuals.Transactions.TOTALPAYMENTAMOUNT",
        "Individuals.Transactions.DCDetails.DESPERCENTAGE",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Optional account number lookup
    lookup_df = load_account_lookup(ACCOUNT_LOOKUP_XLSX if ACCOUNT_LOOKUP_XLSX else None)
    df = apply_account_lookup(df, lookup_df)

    # Simple validation prints before saving
    print(f"[INFO] Final row count: {len(df)}")
    print("[INFO] GN distribution:")
    print(df["Individuals.Transactions.DCDetails.BOOKLABEL"].value_counts(dropna=False))

    # Save to Excel
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Extracted")
    print(f"[DONE] Saved {len(df)} rows to {OUTPUT_XLSX}")

if __name__ == "__main__":
    main()