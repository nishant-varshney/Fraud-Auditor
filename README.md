# NHIS Fraud Auditor Dashboard (Flask + SQLite)

## Overview
Minimal, explainable Fraud Auditor Dashboard using Python, Flask, and SQLite.
It ingests a CSV, stores claims in SQLite, computes a simple Fraud Likelihood Score (0-100),
and provides a dashboard and claims review pages with filters.

## Quickstart
1. Place your CSV into `data/claims.csv`.
2. Create virtualenv & install:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
3. Load data into SQLite:
    python load_data.py
4. Run app:
    flask run
    # or
    python app.py
5. Open http://127.0.0.1:5000/


Files

load_data.py — reads CSV, cleans, computes fraud_score, stores into SQLite.
app.py — Flask web app with routes / and /claims.
database.py — SQLAlchemy engine/session.
templates/ and static/ — UI.
tests/ — pytest unit tests for the fraud scoring function.

Fraud Heuristic (explainable)
Score components:

Amount vs diag average (primary, up to 60 pts) — claims much higher than peers score higher.
Same-day admission/discharge — +10 (suspicious quick procedures).
Very long stay (LOS > 30) — +10.
Absolute amount tiers — +10 (>=10000), +5 (>=5000).
Score is capped between 0–100. This is deterministic and easy to defend.


AI Prompt Journal (required)

Prompt 1: Designing the Fraud Likelihood Heuristic
Prompt:
"I have a healthcare claims dataset with columns: Gender, Diagnosis, Age, Amount, Date Admitted, and Date Discharged. I want a simple, explainable, non-ML heuristic to assign a fraud likelihood score (0-100) to each claim. Suggest a formula that can capture unusual claim amounts relative to typical claims for similar procedures."
Use of AI Output:
The AI suggested scoring rules based on the ratio of the claim amount to an estimated average for similar diagnoses, combined with duration of stay. This formed the core fraud likelihood scoring function implemented in load_data.py.

Prompt 2: Generating Flask Routes for Dashboard
Prompt:
"Generate Flask route code to create a dashboard for a healthcare claims dataset. The dashboard should show: total claims, average claim amount, and percentage of high-fraud-risk claims. Include a chart using Plotly or Matplotlib showing the distribution of fraud scores in categories Low, Medium, High."
Use of AI Output:
AI generated a ready-to-use Flask route with data aggregation and chart generation, which was adapted to integrate with our SQLite backend and templates. This accelerated building the / dashboard view.

Prompt 3: Building the Claims Review Page
Prompt:
"Provide Flask route and template logic to display a paginated, searchable list of claims from SQLite. Each claim should show its Fraud Likelihood Score. Include search filters for Diagnosis, Patient Gender, and Amount."
Use of AI Output:
AI produced pagination logic and search/filter handling, reducing development time while maintaining clean and maintainable code. This was implemented in /claims.

Prompt 4: Test Case Generation for Fraud Score Function
Prompt:
"Generate pytest unit tests for a function compute_fraud_score(claim) that calculates a fraud likelihood score based on Amount and Diagnosis. Include tests for edge cases like zero amounts, unusually high claims, and missing fields."
Use of AI Output:
The AI output formed the tests/test_fraud_score.py, validating the fraud scoring logic against realistic and edge-case claims. This ensured high confidence in the scoring function.


Prompt 5: Comparing Database Choices
Prompt:
"Compare using SQLite vs PostgreSQL for a small, read-heavy Flask dashboard with around 1000-5000 rows. The dashboard requires paginated views, simple aggregation queries, and occasional search filters. Recommend the best choice for speed, simplicity, and deployment."
Use of AI Output:
AI provided a comparison emphasizing SQLite for rapid prototyping, zero configuration, and compatibility with serverless/portable deployments, which guided the decision to use SQLite for this assignment.