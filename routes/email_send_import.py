# routes/email_send_import.py

import os
import re
from datetime import datetime
from flask import Blueprint, request
import pandas as pd
import mysql.connector
from config import Config

email_send_import_bp = Blueprint("email_send_import", __name__, url_prefix="/email_send_import")

EXPECTED_HEADERS = [
    "Sender Email",
    "Receiver Email",
    "First Name",
    "Company",
    "Status",
    "StatusMessage",
    "SentAt",
    "Responds",
    "Subject",
    "Body",
]


def get_db_connection():
    # Uses same DB config pattern as your email_tracking script
    return mysql.connector.connect(
        host=Config.TRACK_DB_HOST,
        user=Config.TRACK_DB_USER,
        password=Config.TRACK_DB_PASS,
        database=Config.TRACK_DB_NAME,
        port=Config.TRACK_DB_PORT,
        use_pure=True,  # helps avoid named-pipe behavior on Windows
    )


def api_response(message, status=200, data=None):
    return {"message": message, "status": status, "data": data or {}}, status


def normalize_header(h: str) -> str:
    return re.sub(r"\s+", " ", (h or "").strip())


def norm_email(val: str) -> str:
    return (val or "").strip().lower()


def parse_sent_at(val):
    if val is None:
        return None

    try:
        if pd.isna(val):
            return None
    except Exception:
        pass

    if isinstance(val, datetime):
        return val

    s = str(val).strip()
    if not s:
        return None

    for fmt in (
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
    ):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass

    try:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()
    except Exception:
        return None


@email_send_import_bp.route("/upload", methods=["POST"])
def upload_email_send_file():
    """
    POST multipart/form-data
      - file: .xlsx/.xls/.csv

    Inserts into:
      email_send_logs

    Also checks:
      email_subscription_preferences (sender_email, receiver_email)
      If exists and is_subscribed = 0 -> responds = 'Unsubscribed'
    """
    if "file" not in request.files:
        return api_response("Missing file in form-data with key 'file'", 400)

    f = request.files["file"]
    if not f or not f.filename:
        return api_response("Empty file", 400)

    ext = os.path.splitext(f.filename.lower())[1]

    conn = None
    cur = None

    try:
        # --------------------
        # Read file
        # --------------------
        if ext in [".xlsx", ".xls"]:
            df = pd.read_excel(f, dtype=str)
        elif ext == ".csv":
            df = pd.read_csv(f, dtype=str)
        else:
            return api_response("Unsupported file type. Upload .xlsx or .csv", 400)

        # Normalize headers
        df.columns = [normalize_header(c) for c in df.columns]

        # Validate headers
        missing = [h for h in EXPECTED_HEADERS if h not in df.columns]
        extra = [c for c in df.columns if c not in EXPECTED_HEADERS]
        if missing:
            return api_response(
                "Invalid file headers",
                400,
                {"missing_headers": missing, "extra_headers": extra, "expected": EXPECTED_HEADERS},
            )

        # Keep expected cols
        df = df[EXPECTED_HEADERS].copy().fillna("")

        # --------------------
        # DB connect
        # --------------------
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)

        rows = []
        skipped = 0
        unsubscribed_override_count = 0

        # --------------------
        # Build insert rows
        # --------------------
        for _, r in df.iterrows():
            sender_email = norm_email(r["Sender Email"])
            receiver_email = norm_email(r["Receiver Email"])

            if not sender_email or not receiver_email:
                skipped += 1
                continue

            responds_value = (r["Responds"] or "").strip()

            # ✅ Unsubscribe check (normalized on both DB + input)
            cur.execute(
                """
                SELECT is_subscribed
                FROM email_subscription_preferences
                WHERE LOWER(TRIM(sender_email)) = %s
                  AND LOWER(TRIM(receiver_email)) = %s
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (sender_email, receiver_email),
            )
            pref = cur.fetchone()

            if pref and str(pref.get("is_subscribed")) == "0":
                responds_value = "Unsubscribed"
                unsubscribed_override_count += 1

            rows.append(
                (
                    sender_email,
                    receiver_email,
                    (r["First Name"] or "").strip() or None,
                    (r["Company"] or "").strip() or None,
                    (r["Status"] or "").strip() or None,
                    (r["StatusMessage"] or "").strip() or None,
                    parse_sent_at(r["SentAt"]),
                    responds_value or None,
                    (r["Subject"] or "").strip() or None,
                    (r["Body"] or "").strip() or None,
                )
            )

        if not rows:
            return api_response("No valid rows found to insert", 400, {"skipped": skipped})

        # --------------------
        # Insert into DB
        # --------------------
        insert_sql = """
            INSERT INTO email_send_logs
            (sender_email, receiver_email, first_name, company, status, status_message,
             sent_at, responds, subject, body)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        cur.executemany(insert_sql, rows)
        conn.commit()

        return api_response(
            "Imported successfully",
            200,
            {
                "inserted": cur.rowcount,
                "skipped": skipped,
                "total_rows_in_file": int(len(df)),
                "unsubscribed_overrides": unsubscribed_override_count,
            },
        )

    except Exception as e:
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
        return api_response(f"Import failed: {str(e)}", 500)

    finally:
        try:
            if cur:
                cur.close()
        except Exception:
            pass
        try:
            if conn:
                conn.close()
        except Exception:
            pass

@email_send_import_bp.route("/list", methods=["POST"])
def list_email_logs():
    data = request.get_json(silent=True) or {}

    page = int(data.get("page", 1))
    per_page = int(data.get("per_page", 10))
    date_val = (data.get("date") or "").strip()  # ✅ single date: YYYY-MM-DD
    sender_email = (data.get("sender_email") or "").strip().lower()
    receiver_email = (data.get("receiver_email") or "").strip().lower()

    if page < 1:
        page = 1
    if per_page < 1:
        per_page = 10

    offset = (page - 1) * per_page

    conn = None
    cur = None

    try:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)

        where_clauses = []
        params = []

        # ✅ Single date filter
        if date_val:
            where_clauses.append("DATE(sent_at) = %s")
            params.append(date_val)

        if sender_email:
            where_clauses.append("LOWER(sender_email) = %s")
            params.append(sender_email)

        if receiver_email:
            where_clauses.append("LOWER(receiver_email) = %s")
            params.append(receiver_email)

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        # Total count
        count_sql = f"SELECT COUNT(*) AS total FROM email_send_logs {where_sql}"
        cur.execute(count_sql, params)
        total_records = int(cur.fetchone()["total"])

        # Data
        data_sql = f"""
            SELECT *
            FROM email_send_logs
            {where_sql}
            ORDER BY id DESC
            LIMIT %s OFFSET %s
        """
        cur.execute(data_sql, params + [per_page, offset])
        rows = cur.fetchall()

        return api_response(
            "Fetched successfully",
            200,
            {
                "filters_applied": {
                    "date": date_val or None,
                    "sender_email": sender_email or None,
                    "receiver_email": receiver_email or None,
                },
                "pagination": {
                    "page": page,
                    "per_page": per_page,
                    "total_records": total_records,
                    "total_pages": (total_records + per_page - 1) // per_page,
                },
                "records": rows,
            },
        )

    except Exception as e:
        return api_response(f"Fetch failed: {str(e)}", 500)

    finally:
        try:
            if cur:
                cur.close()
        except Exception:
            pass
        try:
            if conn:
                conn.close()
        except Exception:
            pass
