"""
Load claims CSV into SQLite and compute fraud_score.
Run this before starting the Flask app (it creates/refreshes data/claims.db).
"""

from log_config import configure_logging
logger = configure_logging()

import os
import pandas as pd
from dateutil import parser
from sqlalchemy import text, MetaData
from database import engine


DATA_DIR = "data"
CSV_PATH = os.path.join(DATA_DIR, "claims.csv")
DB_PATH = os.path.join(DATA_DIR, "claims.db")

metadata = MetaData()


# ----------------------------------------------------
# Utility Functions
# ----------------------------------------------------

def _clean_amount(x):
    if pd.isna(x):
        return None
    s = str(x).strip().replace(",", "").replace("₦", "").replace("$", "")
    try:
        return float(s)
    except Exception:
        logger.warning(f"Unclean amount value encountered: {x}")
        return None


def _parse_date(d):
    if pd.isna(d) or str(d).strip() == "":
        return None
    try:
        return parser.parse(str(d), dayfirst=False).date()
    except Exception:
        try:
            return parser.parse(str(d), dayfirst=True).date()
        except Exception:
            logger.warning(f"Invalid date encountered: {d}")
            return None


def compute_fraud_score(amount, diag_avg, los_days):
    if amount is None:
        amount = 0.0

    if diag_avg is None or diag_avg == 0 or diag_avg != diag_avg:
        ratio = 1.0
    else:
        ratio = amount / diag_avg if diag_avg > 0 else 1.0

    score = int(min(60, ratio * 20))

    if los_days == 0:
        score += 10
    if los_days and los_days > 30:
        score += 10
    if amount >= 10000:
        score += 10
    elif amount >= 5000:
        score += 5

    return max(0, min(100, score))


# ----------------------------------------------------
# Main Loader
# ----------------------------------------------------

def prepare_and_store(force=False):
    logger.info("Starting data load process...")

    os.makedirs(DATA_DIR, exist_ok=True)

    if not os.path.exists(CSV_PATH):
        logger.error(f"CSV not found at {CSV_PATH}")
        raise FileNotFoundError(f"CSV not found at {CSV_PATH}. Please place claims.csv there.")

    # Load CSV
    logger.info("Reading CSV file...")
    try:
        df = pd.read_csv(CSV_PATH, sep=None, engine="python")
    except Exception:
        df = pd.read_csv(CSV_PATH, engine="python")

    logger.info("Normalizing column names...")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Cleaning
    logger.info("Cleaning amount, dates, and computing LOS...")
    df["amount_clean"] = df.get("amount", "").apply(_clean_amount)
    df["date_admitted_parsed"] = df.get("date_admitted", "").apply(_parse_date)
    df["date_discharged_parsed"] = df.get("date_discharged", "").apply(_parse_date)

    df["los"] = (
        pd.to_datetime(df["date_discharged_parsed"], errors="coerce")
        - pd.to_datetime(df["date_admitted_parsed"], errors="coerce")
    ).dt.days.fillna(0).astype(int)

    df["diagnosis_clean"] = df["diagnosis"].astype(str).str.strip().str.upper()

    # Diagnosis averages
    logger.info("Computing diagnosis averages...")
    diag_avg = df.groupby("diagnosis_clean")["amount_clean"].mean().to_dict()
    df["diag_avg"] = df["diagnosis_clean"].map(diag_avg)

    # Fraud score
    logger.info("Computing fraud scores...")
    df["fraud_score"] = df.apply(
        lambda r: compute_fraud_score(r["amount_clean"], r["diag_avg"], int(r["los"])),
        axis=1
    )

    df["fraud_category"] = df["fraud_score"].apply(
        lambda s: "Low" if s <= 25 else ("Medium" if s <= 75 else "High")
    )

    # Dates
    df["date_admitted_iso"] = df["date_admitted_parsed"].astype(str).replace("NaT", "")
    df["date_discharged_iso"] = df["date_discharged_parsed"].astype(str).replace("NaT", "")

    # Drop table + recreate
    logger.info("Recreating SQLite table...")
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS claims"))

    # Preparing final DataFrame
    logger.info("Preparing DataFrame for SQLite insert...")
    keep_cols = [
        "s/n", "gender", "diagnosis", "age", "amount_clean",
        "date_admitted_iso", "date_discharged_iso", "los",
        "diagnosis_clean", "diag_avg", "fraud_score", "fraud_category"
    ]
    store_df = df[[c for c in keep_cols if c in df.columns]].copy()

    store_df = store_df.rename(columns={
        "s/n": "sn",
        "amount_clean": "amount",
        "date_admitted_iso": "date_admitted",
        "date_discharged_iso": "date_discharged",
        "diagnosis_clean": "diagnosis_canon"
    })

    logger.info("Writing into SQLite database...")
    store_df.to_sql("claims", con=engine, index=False, if_exists="replace")

    logger.info(f"SUCCESS: Loaded {len(store_df)} rows into SQLite at {DB_PATH}")


# ----------------------------------------------------
# Entry point
# ----------------------------------------------------
if __name__ == "__main__":
    prepare_and_store(force=True)
