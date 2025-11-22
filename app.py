import math
import logging
from flask import Flask, render_template, request, g, jsonify
from sqlalchemy import text
from database import engine, SessionLocal
import os
from log_config import configure_logging
logger = configure_logging()

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True


def get_db():
    if "db" not in g:
        logger.debug("Creating new DB session")
        g.db = SessionLocal()
    return g.db


@app.teardown_appcontext
def teardown_db(exception=None):
    db = g.pop("db", None)
    if db is not None:
        logger.debug("Closing DB session")
        db.close()


@app.route("/")
def index():
    logger.info("Loading dashboard (index page)")
    try:
        with engine.connect() as conn:
            logger.debug("Executing dashboard metrics queries")

            total = conn.execute(text("SELECT COUNT(*) FROM claims")).scalar() or 0
            avg_amt = conn.execute(text("SELECT AVG(amount) FROM claims")).scalar() or 0.0
            high_count = conn.execute(
                text("SELECT COUNT(*) FROM claims WHERE fraud_score > 75")
            ).scalar() or 0

            logger.debug(f"Total Claims: {total}, Avg Amount: {avg_amt}, High Risk Count: {high_count}")

            dist_rows = conn.execute(
                text("SELECT fraud_category, COUNT(*) FROM claims GROUP BY fraud_category")
            ).all()
            dist = {r[0]: r[1] for r in dist_rows}

            top_rows = conn.execute(
                text("SELECT rowid, diagnosis, amount, fraud_score, fraud_category "
                     "FROM claims ORDER BY fraud_score DESC LIMIT 10")
            ).all()

        pct_flagged = round((high_count / total * 100), 2) if total else 0

        return render_template(
            "index.html",
            total_claims=total,
            avg_amount=round(avg_amt, 2),
            high_count=high_count,
            pct_flagged=pct_flagged,
            dist=dist,
            top_rows=top_rows
        )

    except Exception as e:
        logger.error("Error loading index page", exc_info=True)
        return "Internal Server Error", 500


@app.route("/claims")
def claims():
    q = request.args.get("q", "").strip()
    category = request.args.get("category", "").strip()

    logger.info(f"Claims page requested | Search: '{q}', Category: '{category}'")

    sql = """
        SELECT rowid, sn, gender, diagnosis, amount,
               date_admitted, date_discharged, los,
               fraud_score, fraud_category
        FROM claims WHERE 1=1
    """

    params = {}

    if q:
        logger.debug(f"Applying search filter: {q}")
        sql += " AND (UPPER(diagnosis) LIKE :q OR UPPER(gender) LIKE :q)"
        params["q"] = f"%{q.upper()}%"

    if category:
        logger.debug(f"Applying category filter: {category}")
        sql += " AND fraud_category = :cat"
        params["cat"] = category

    # Pagination
    page = int(request.args.get("page", 1) or 1)
    per_page = 10
    offset = (page - 1) * per_page

    logger.debug(f"Pagination → Page: {page}, Offset: {offset}, Per Page: {per_page}")

    try:
        count_sql = f"SELECT COUNT(*) FROM ({sql})"

        with engine.connect() as conn:
            logger.debug("Executing count query for pagination")
            total = conn.execute(text(count_sql), params).scalar()

            logger.debug("Executing claims fetch query")
            rows = conn.execute(
                text(sql + " LIMIT :limit OFFSET :offset"),
                {**params, "limit": per_page, "offset": offset}
            ).all()

        pages = math.ceil(total / per_page)

        logger.debug(f"Total rows: {total}, Total pages: {pages}")

        return render_template(
            "claims.html",
            claims=rows,
            page=page,
            pages=pages,
            total=total,
            q=q,
            category=category
        )

    except Exception as e:
        logger.error("Error loading claims page", exc_info=True)
        return "Internal Server Error", 500


@app.route("/api/claims")
def api_claims():
    offset = int(request.args.get("offset", 0))
    limit = int(request.args.get("limit", 100))

    logger.info(f"API /api/claims called | Offset: {offset}, Limit: {limit}")

    try:
        with engine.connect() as conn:
            logger.debug("Fetching claims via API")
            rows = conn.execute(
                text("SELECT rowid, sn, gender, diagnosis, amount, date_admitted, "
                     "date_discharged, los, fraud_score, fraud_category "
                     "FROM claims LIMIT :limit OFFSET :offset"),
                {"limit": limit, "offset": offset}
            ).all()

        results = [dict(r) for r in rows]
        logger.debug(f"API returning {len(results)} records")

        return jsonify(results)

    except Exception as e:
        logger.error("API error: /api/claims failed", exc_info=True)
        return jsonify({"error": "Internal Server Error"}), 500


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    logger.info("Starting Flask app in DEBUG mode")
    app.run(debug=True)