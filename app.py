# app.py
import math
from flask import Flask, render_template, request, g, jsonify
from sqlalchemy import text
from database import engine, SessionLocal
import os

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True

def get_db():
    if "db" not in g:
        g.db = SessionLocal()
    return g.db

@app.teardown_appcontext
def teardown_db(exception=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()

@app.route("/")
def index():
    # overall metrics and distribution
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM claims")).scalar() or 0
        avg_amt = conn.execute(text("SELECT AVG(amount) FROM claims")).scalar() or 0.0
        high_count = conn.execute(text("SELECT COUNT(*) FROM claims WHERE fraud_score > 75")).scalar() or 0

        # distribution
        dist_rows = conn.execute(text("SELECT fraud_category, COUNT(*) as cnt FROM claims GROUP BY fraud_category")).all()
        dist = {r[0]: r[1] for r in dist_rows}

        # top suspicious claims (by score)
        top_rows = conn.execute(text("SELECT rowid, diagnosis, amount, fraud_score, fraud_category FROM claims ORDER BY fraud_score DESC LIMIT 10")).all()

    pct_flagged = round((high_count / total * 100), 2) if total else 0

    return render_template("index.html",
                           total_claims=total,
                           avg_amount=round(avg_amt or 0, 2),
                           high_count=high_count,
                           pct_flagged=pct_flagged,
                           dist=dist,
                           top_rows=top_rows)

@app.route("/claims")
def claims():
    q = request.args.get("q", "").strip()
    category = request.args.get("category", "").strip()

    sql = "SELECT rowid, sn, gender, diagnosis, amount, date_admitted, date_discharged, los, fraud_score, fraud_category FROM claims WHERE 1=1"
    params = {}

    if q:
        sql += " AND (UPPER(diagnosis) LIKE :q OR UPPER(gender) LIKE :q)"
        params["q"] = f"%{q.upper()}%"
    if category:
        sql += " AND fraud_category = :cat"
        params["cat"] = category

    # pagination
    page = int(request.args.get("page", 1) or 1)
    per_page = 10
    offset = (page - 1) * per_page

    count_sql = f"SELECT COUNT(*) FROM ({sql})"
    with engine.connect() as conn:
        total = conn.execute(text(count_sql), params).scalar()
        rows = conn.execute(text(sql + " LIMIT :limit OFFSET :offset"), {**params, "limit": per_page, "offset": offset}).all()

    pages = math.ceil(total / per_page) if per_page else 1

    return render_template("claims.html", claims=rows, page=page, pages=pages, total=total, q=q, category=category)

@app.route("/api/claims")
def api_claims():
    offset = int(request.args.get("offset", 0))
    limit = int(request.args.get("limit", 100))
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT rowid, sn, gender, diagnosis, amount, date_admitted, date_discharged, los, fraud_score, fraud_category FROM claims LIMIT :limit OFFSET :offset"),
                            {"limit": limit, "offset": offset}).all()
        results = [dict(r) for r in rows]
    return jsonify(results)

if __name__ == "__main__":
    # ensure DB exists
    os.makedirs("data", exist_ok=True)
    app.run(debug=True)
