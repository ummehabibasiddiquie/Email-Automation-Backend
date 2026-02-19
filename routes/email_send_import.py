import os
import re
import hashlib
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

# =========================
# DB + helpers
# =========================
def get_db_connection():
    conn = mysql.connector.connect(
        host=Config.TRACK_DB_HOST,
        user=Config.TRACK_DB_USER,
        password=Config.TRACK_DB_PASS,
        database=Config.TRACK_DB_NAME,
        port=Config.TRACK_DB_PORT,
        use_pure=True,
    )

    # 🔥 FORCE IST TIMEZONE FOR SESSION
    cursor = conn.cursor()
    cursor.execute("SET time_zone = '+05:30'")
    cursor.close()

    return conn


def api_response(message, status=200, data=None):
    return {"message": message, "status": status, "data": data or {}}, status

def normalize_header(h: str) -> str:
    return re.sub(r"\s+", " ", (h or "").strip())

def norm_email(val: str) -> str:
    return (val or "").strip().lower()

def norm_text(val: str):
    s = (val or "").strip()
    return s if s else None

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

def is_real_response(responds: str | None) -> bool:
    """
    Treat these as NOT a real response:
      - None / empty
      - 'No Response Yet' (your file default)
    Everything else counts as response (including 'Unsubscribed')
    """
    if not responds:
        return False
    v = responds.strip().lower()
    return v not in ("", "no response yet")


def is_unsubscribe_response(responds: str | None) -> bool:
    if not responds:
        return False
    val = responds.strip().lower()
    return val in ("unsubscribed", "not interested")


# =========================
# DEDUPE (includes email_type)
# =========================
def make_dedupe_key(sender: str, receiver: str, sent_at: datetime, email_type: str):
    if not sent_at:
        return None
    sent = sent_at.strftime("%Y-%m-%d %H:%M:%S")
    raw = "|".join([
        (sender or "").strip().lower(),
        (receiver or "").strip().lower(),
        (email_type or "").strip().upper(),
        sent,
    ])
    return hashlib.md5(raw.encode("utf-8")).hexdigest()

# =========================
# Upload API
# =========================
@email_send_import_bp.route("/upload", methods=["POST"])
def upload_email_send_file():
    """
    POST multipart/form-data:
      - file: .xlsx/.xls/.csv
      - email_type: GOLY | MPLY   (from dropdown)

    Behavior:
      - Identity = sender + receiver + email_type + sent_at
      - If exists:
          - updates allowed fields if changed
          - sets updated_at = NOW() only when responds becomes a real response (or changes)
      - Unsubscribe override:
          if email_subscription_preferences.is_subscribed = 0
          -> Responds='Unsubscribed' (and updated_at NOW)
    """

    if "file" not in request.files:
        return api_response("Missing file in form-data with key 'file'", 400)

    email_type = (request.form.get("email_type") or "").strip().upper()
    if email_type not in ["GOLY", "MPLY"]:
        return api_response("Invalid email_type. Use GOLY or MPLY", 400)

    f = request.files["file"]
    if not f or not f.filename:
        return api_response("Empty file", 400)

    ext = os.path.splitext(f.filename.lower())[1]
    conn = None
    cur = None

    try:
        # Read file
        if ext in [".xlsx", ".xls"]:
            df = pd.read_excel(f, dtype=str)
        elif ext == ".csv":
            df = pd.read_csv(f, dtype=str)
        else:
            return api_response("Unsupported file type. Upload .xlsx or .csv", 400)

        df.columns = [normalize_header(c) for c in df.columns]

        missing = [h for h in EXPECTED_HEADERS if h not in df.columns]
        if missing:
            return api_response(
                "Invalid file headers",
                400,
                {"missing_headers": missing, "expected": EXPECTED_HEADERS},
            )

        df = df[EXPECTED_HEADERS].copy().fillna("")

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)

        skipped = 0
        inserted = 0
        updated = 0
        duplicates_no_change = 0
        unsubscribed_override_count = 0

        candidates = []
        dedupe_keys = []

        # Build candidates
        for _, r in df.iterrows():
            sender_email = norm_email(r["Sender Email"])
            receiver_email = norm_email(r["Receiver Email"])

            if not sender_email or not receiver_email:
                skipped += 1
                continue

            first_name = norm_text(r["First Name"])
            company = norm_text(r["Company"])
            status = norm_text(r["Status"])
            status_message = norm_text(r["StatusMessage"])
            sent_at = parse_sent_at(r["SentAt"])
            subject = norm_text(r["Subject"])
            body = norm_text(r["Body"])
            responds_value = (r["Responds"] or "").strip() or None

            # ✅ If reply is "unsubscribed" or "not interested", update subscription
            if is_unsubscribe_response(responds_value):
                cur.execute(
                    """
                    INSERT INTO email_subscription_preferences
                      (sender_email, receiver_email, is_subscribed, updated_at)
                    VALUES (%s, %s, 0, %s)
                    ON DUPLICATE KEY UPDATE
                      is_subscribed=0,
                      updated_at=VALUES(updated_at)
                    """,
                    (sender_email, receiver_email, datetime.now()),
                )
                unsubscribed_override_count += 1
                status_message = "Receiver Unsubscribed via mail"

            dkey = make_dedupe_key(sender_email, receiver_email, sent_at, email_type)
            if not dkey:
                skipped += 1
                continue

            # Unsubscribe override
            cur.execute(
                """
                SELECT is_subscribed
                FROM email_subscription_preferences
                WHERE LOWER(TRIM(sender_email))=%s
                  AND LOWER(TRIM(receiver_email))=%s
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (sender_email, receiver_email),
            )
            pref = cur.fetchone()
            if pref and str(pref.get("is_subscribed")) == "0":
                if not is_unsubscribe_response(responds_value):
                    unsubscribed_override_count += 1
                responds_value = "Unsubscribed"
                status_message = "Receiver Unsubscribed via mail"


            candidates.append({
                "dedupe_key": dkey,
                "sender_email": sender_email,
                "receiver_email": receiver_email,
                "email_type": email_type,
                "first_name": first_name,
                "company": company,
                "status": status,
                "status_message": status_message,
                "sent_at": sent_at,
                "responds": responds_value,
                "subject": subject,
                "body": body,
            })
            dedupe_keys.append(dkey)

        if not candidates:
            return api_response(
                "No valid rows found to insert/update (SentAt missing?)",
                400,
                {"skipped": skipped},
            )

        # Fetch existing rows by dedupe_key
        existing_map = {}
        CHUNK = 500
        for i in range(0, len(dedupe_keys), CHUNK):
            chunk = dedupe_keys[i:i+CHUNK]
            placeholders = ",".join(["%s"] * len(chunk))
            cur.execute(
                f"""
                SELECT id, dedupe_key, first_name, company, status, status_message,
                       responds, subject, body, updated_at
                FROM email_send_logs
                WHERE dedupe_key IN ({placeholders})
                """,
                chunk,
            )
            for row in cur.fetchall():
                existing_map[row["dedupe_key"]] = row

        insert_rows = []
        update_rows = []

        def same(a, b):
            return (a or None) == (b or None)

        for c in candidates:
            ex = existing_map.get(c["dedupe_key"])

            # For new records, always set the updated_at timestamp.
            if not ex:
                insert_rows.append((
                    c["sender_email"],
                    c["receiver_email"],
                    c["email_type"],
                    c["first_name"],
                    c["company"],
                    c["status"],
                    c["status_message"],
                    c["sent_at"],
                    c["responds"],
                    datetime.now(),   # ✅ Always set updated_at for new records
                    c["subject"],
                    c["body"],
                    c["dedupe_key"],
                ))
                continue

            # Determine if any data fields have changed
            changed = False
            for field in ["first_name","company","status","status_message","responds","subject","body"]:
                if not same(ex.get(field), c.get(field)):
                    changed = True
                    break

            # For existing records, decide whether to update the timestamp
            set_updated_at = ex.get("updated_at")
            # A valid date from DB will be a datetime object. An invalid one might be a string '0000-00-00...'.
            # If there's no valid timestamp, or if the data has changed, update it.
            if not isinstance(set_updated_at, datetime) or changed:
                set_updated_at = datetime.now()

            if not changed and isinstance(ex.get("updated_at"), datetime):
                 # If data hasn't changed and a valid timestamp exists, no need to update.
                duplicates_no_change += 1
                continue

            update_rows.append((
                c["first_name"],
                c["company"],
                c["status"],
                c["status_message"],
                c["responds"],
                set_updated_at,        # ✅ Set updated_at if missing or if data changed
                c["subject"],
                c["body"],
                int(ex["id"]),
            ))

        # Execute DB writes
        if insert_rows:
            cur.executemany(
                """
                INSERT INTO email_send_logs
                (sender_email, receiver_email, email_type, first_name, company, status, status_message,
                 sent_at, responds, updated_at, subject, body, dedupe_key)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                insert_rows,
            )
            inserted = cur.rowcount

        if update_rows:
            cur.executemany(
                """
                UPDATE email_send_logs
                SET first_name=%s,
                    company=%s,
                    status=%s,
                    status_message=%s,
                    responds=%s,
                    updated_at=%s,
                    subject=%s,
                    body=%s
                WHERE id=%s
                """,
                update_rows,
            )
            updated = cur.rowcount

        conn.commit()

        return api_response("Imported successfully", 200, {
            "email_type": email_type,
            "inserted": inserted,
            "updated": updated,
            "duplicates_no_change": duplicates_no_change,
            "skipped": skipped,
            "total_rows_in_file": int(len(df)),
            "unsubscribed_overrides": unsubscribed_override_count,
        })

    except mysql.connector.IntegrityError as ie:
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
        return api_response(f"Import failed (duplicate key): {str(ie)}", 409)

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

# =========================
# Report API (Sent / Responds)
# =========================
# =========================
# Report API (Sent / Responds) + Monthly Stats (Current Month Only)
# =========================
@email_send_import_bp.route("/report", methods=["POST"])
def email_report():
    """
    POST JSON:
      {
        "type": "sent" | "responds",
        "page": 1,
        "per_page": 10,
        "email_type": "GOLY" | "MPLY" | "",

        "date": "YYYY-MM-DD",                 (optional single date)
        "date_from": "YYYY-MM-DD",            (optional range)
        "date_to": "YYYY-MM-DD"
      }

    Sent tab filters by DATE(sent_at)
    Responds tab filters by DATE(updated_at) and responds not empty
    Also returns monthly stats for current month (no date filters applied to monthly except email_type).
    """

    data = request.get_json(silent=True) or {}

    report_type = (data.get("type") or "").lower()
    page = int(data.get("page", 1))
    per_page = int(data.get("per_page", 10))

    email_type = (data.get("email_type") or "").strip().upper()
    date = (data.get("date") or "").strip()
    date_from = (data.get("date_from") or "").strip()
    date_to = (data.get("date_to") or "").strip()
    responds_filter = (data.get("responds_filter") or "").strip()

    if report_type not in ["sent", "responds"]:
        return api_response("Invalid type. Use 'sent' or 'responds'", 400)

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

        # ✅ Filter by email_type (GOLY/MPLY)
        if email_type:
            where_clauses.append("email_type = %s")
            params.append(email_type)

        # ✅ Filter by responds_filter
        # ✅ Filter by responds_filter (ONLY 4 supported values)
        if responds_filter:
            rf = responds_filter.strip().lower()

            if rf == "no response yet":
                where_clauses.append(
                    "(responds IS NULL OR TRIM(responds) = '' OR LOWER(TRIM(responds)) = 'no response yet')"
                )

            elif rf == "unsubscribed":
                where_clauses.append("LOWER(TRIM(responds)) = 'unsubscribed'")

            elif rf == "positive response":
                where_clauses.append("LOWER(TRIM(responds)) = 'positive response'")

            elif rf == "response":
                # ✅ ONLY exact 'Response'
                where_clauses.append("LOWER(TRIM(responds)) = 'response'")

            else:
                return api_response(
                    "Invalid responds_filter. Use: No Response Yet | Response | Positive Response | Unsubscribed",
                    400,
                )

        # ----------------------------
        # SENT TAB
        # ----------------------------
        if report_type == "sent":
            if date:
                where_clauses.append("DATE(sent_at) = %s")
                params.append(date)

            if date_from and date_to:
                where_clauses.append("DATE(sent_at) BETWEEN %s AND %s")
                params.extend([date_from, date_to])

            where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

            cur.execute(
                f"SELECT COUNT(*) AS total FROM email_send_logs {where_sql}",
                params,
            )
            total_records = int(cur.fetchone()["total"])

            cur.execute(
                f"""
                SELECT
                    sender_email,
                    receiver_email,
                    email_type,
                    subject,
                    status,
                    status_message,
                    sent_at,
                    responds
                FROM email_send_logs
                {where_sql}
                ORDER BY sent_at DESC
                LIMIT %s OFFSET %s
                """,
                params + [per_page, offset],
            )
            rows = cur.fetchall()
            
            for r in rows:
                if r.get("updated_at"):
                    r["updated_at"] = r["updated_at"].strftime("%Y-%m-%d %H:%M:%S")

        # ----------------------------
        # RESPONDS TAB
        # ----------------------------
        else:
            if not responds_filter: # Default filter for responds tab if no specific filter is given
                where_clauses.append("responds IS NOT NULL")
                where_clauses.append("responds <> ''")
                where_clauses.append("LOWER(TRIM(responds)) <> 'no response yet'")

            if date:
                where_clauses.append("DATE(updated_at) = %s")
                params.append(date)

            if date_from and date_to:
                where_clauses.append("DATE(updated_at) BETWEEN %s AND %s")
                params.extend([date_from, date_to])

            where_sql = "WHERE " + " AND ".join(where_clauses)

            cur.execute(
                f"SELECT COUNT(*) AS total FROM email_send_logs {where_sql}",
                params,
            )
            total_records = int(cur.fetchone()["total"])

            cur.execute(
                f"""
                SELECT
                    sender_email,
                    receiver_email,
                    email_type,
                    responds,
                    subject,
                    body,
                    updated_at
                FROM email_send_logs
                {where_sql}
                ORDER BY updated_at DESC
                LIMIT %s OFFSET %s
                """,
                params + [per_page, offset],
            )
            rows = cur.fetchall()
            
            for r in rows:
                if r.get("sent_at"):
                    r["sent_at"] = r["sent_at"].strftime("%Y-%m-%d %H:%M:%S")

        # =========================
        # Monthly Stats (Current Month Only)
        # =========================
        monthly_where = []
        monthly_params = []

        if email_type:
            monthly_where.append("email_type = %s")
            monthly_params.append(email_type)

        monthly_where.append("sent_at >= DATE_FORMAT(NOW(), '%Y-%m-01')")
        monthly_where.append("sent_at <  DATE_ADD(DATE_FORMAT(NOW(), '%Y-%m-01'), INTERVAL 1 MONTH)")
        monthly_where_sql = "WHERE " + " AND ".join(monthly_where)

        cur.execute(
            f"""
            SELECT
              COUNT(*) AS monthly_sent,

              SUM(CASE
                    WHEN LOWER(TRIM(responds)) = 'unsubscribed'
                    THEN 1 ELSE 0
                  END) AS monthly_unsubscribed,

              SUM(CASE
                    WHEN responds IS NOT NULL
                     AND TRIM(responds) <> ''
                     AND LOWER(TRIM(responds)) <> 'no response yet'
                     AND LOWER(TRIM(responds)) <> 'unsubscribed'
                    THEN 1 ELSE 0
                  END) AS monthly_positive_responds,

              SUM(CASE
                    WHEN responds IS NULL
                      OR TRIM(responds) = ''
                      OR LOWER(TRIM(responds)) = 'no response yet'
                    THEN 1 ELSE 0
                  END) AS monthly_not_responds
            FROM email_send_logs
            {monthly_where_sql}
            """,
            monthly_params,
        )
        monthly_data = cur.fetchone() or {}

        return api_response(
            f"{report_type.capitalize()} report fetched successfully",
            200,
            {
                "type": report_type,
                "records": rows,
                "pagination": {
                    "page": page,
                    "per_page": per_page,
                    "total_records": total_records,
                    "total_pages": (total_records + per_page - 1) // per_page,
                },
                "filters_applied": {
                    "email_type": email_type or None,
                    "date": date or None,
                    "date_from": date_from or None,
                    "date_to": date_to or None,
                    "responds_filter": responds_filter or None,
                },
                "monthly_stats": {
                    "monthly_sent": int(monthly_data.get("monthly_sent") or 0),
                    "monthly_unsubscribed": int(monthly_data.get("monthly_unsubscribed") or 0),
                    "monthly_positive_responds": int(monthly_data.get("monthly_positive_responds") or 0),
                    "monthly_not_responds": int(monthly_data.get("monthly_not_responds") or 0),
                },
            },
        )

    except Exception as e:
        return api_response(f"Report fetch failed: {str(e)}", 500)

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

@email_send_import_bp.route("/responds-options", methods=["GET"])
def get_responds_options():
    """
    Returns dropdown options for responds filter
    """

    options = [
        {"label": "No Response Yet"},
        {"label": "Response"},
        {"label": "Positive Response"},
        {"label": "Unsubscribed"},
    ]

    return api_response(
        "Responds options fetched successfully",
        200,
        {"options": options},
    )
