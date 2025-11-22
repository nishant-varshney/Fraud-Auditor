# load_data.py
"""
Load claims CSV into SQLite and compute fraud_score.
Run this before starting the Flask app (it creates/refreshes data/claims.db).
"""

import os
import pandas as pd
from dateutil import parser
from sqlalchemy import Column, Integer, Float, String, Date, create_engine
from sqlalchemy.orm import Session
from database import Base, engine
from sqlalchemy import MetaData, Table

DATA_DIR = "data"
CSV_PATH = os.path.join(DATA_DIR, "claims.csv")
DB_PATH = os.path.join(DATA_DIR, "claims.db")

# Define a table using SQLAlchemy Core meta for simple write (keeps dependencies minimal)
metadata = MetaData()

def _clean_amount(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    s = s.replace(",", "").replace("₦", "").replace("$", "")
    try:
        return float(s)
    except Exception:
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
            return None

def compute_fraud_score(amount, diag_avg, los_days):
    """
    Explainable heuristic (0-100):
    - Primary signal: amount relative to avg for same diagnosis (weight 0.6, up to 60 pts)
    - Secondary signals:
        * same-day admission/discharge (10 pts)
        * long stay (los > 30) (10 pts)
        * absolute amount tiers (>=10000 -> +10, >=5000 -> +5)
    Steps:
      ratio_score = min(60, int( (amount/diag_avg if diag_avg and diag_avg>0 else 1) * 20 ))
      then add other flags, cap at 100.
    This is simple, deterministic, and explainable.
    """
    if amount is None:
        amount = 0.0
    if diag_avg is None or diag_avg == 0 or diag_avg != diag_avg:
        ratio = 1.0
    else:
        ratio = amount / diag_avg if diag_avg > 0 else 1.0

    # ratio contribution: scale by 20, cap at 60
    ratio_score = int(min(60, ratio * 20))
    score = ratio_score

    # same-day or zero-length stay handled outside where los_days == 0 (but keep check)
    if los_days == 0:
        score += 10

    if los_days is not None and los_days > 30:
        score += 10

    if amount >= 10000:
        score += 10
    elif amount >= 5000:
        score += 5

    # ensure bounds
    score = max(0, min(100, int(score)))
    return score

def prepare_and_store(force=False):
    os.makedirs(DATA_DIR, exist_ok=True)

    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found at {CSV_PATH}. Please place claims.csv there.")

    # read CSV - allow tab or comma separated
    try:
        df = pd.read_csv(CSV_PATH, sep=None, engine="python")
    except Exception:
        df = pd.read_csv(CSV_PATH, engine="python")

    # normalize column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # clean fields
    df["amount_clean"] = df.get("amount", "").apply(_clean_amount)
    df["date_admitted_parsed"] = df.get("date_admitted", "").apply(_parse_date)
    df["date_discharged_parsed"] = df.get("date_discharged", "").apply(_parse_date)
    # length of stay in days (int)
    df["los"] = (pd.to_datetime(df["date_discharged_parsed"], errors="coerce") - pd.to_datetime(df["date_admitted_parsed"], errors="coerce")).dt.days
    df["los"] = df["los"].fillna(0).astype(int)

    # canonical diagnosis
    df["diagnosis_clean"] = df.get("diagnosis", "").astype(str).str.strip().str.upper()

    # compute diagnosis average charge (skip nulls)
    diag_avg = df.groupby("diagnosis_clean")["amount_clean"].mean().to_dict()

    # compute fraud_score
    df["diag_avg"] = df["diagnosis_clean"].map(diag_avg)
    df["fraud_score"] = df.apply(lambda r: compute_fraud_score(r["amount_clean"] or 0.0, r["diag_avg"], int(r["los"])), axis=1)

    # fraud category
    def cat(s):
        if s <= 25:
            return "Low"
        if s <= 75:
            return "Medium"
        return "High"
    df["fraud_category"] = df["fraud_score"].apply(cat)

    # convert dates to ISO strings for storage
    df["date_admitted_iso"] = df["date_admitted_parsed"].astype(str).replace("NaT", "")
    df["date_discharged_iso"] = df["date_discharged_parsed"].astype(str).replace("NaT", "")

    # create table and insert into SQLite
    # We'll use pandas.to_sql for simplicity
    from sqlalchemy import text
    # drop and recreate
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS claims"))
    # select subset of columns to store
    store_df = df[[
        col for col in [
            "s/n","gender","diagnosis","age","amount_clean",
            "date_admitted_iso","date_discharged_iso","los",
            "diagnosis_clean","diag_avg","fraud_score","fraud_category"
        ] if col in df.columns or col=="s/n"
    ]].copy()

    # rename for nicer DB column names
    rename_map = {
        "s/n": "sn",
        "amount_clean": "amount",
        "date_admitted_iso": "date_admitted",
        "date_discharged_iso": "date_discharged",
        "diagnosis_clean": "diagnosis_canon",
        "diag_avg": "diag_avg"
    }
    store_df = store_df.rename(columns=rename_map)

    store_df.to_sql("claims", con=engine, index=False, if_exists="replace")

    print(f"Loaded {len(store_df)} rows into SQLite at {DB_PATH}.")

if __name__ == "__main__":
    prepare_and_store(force=True)
