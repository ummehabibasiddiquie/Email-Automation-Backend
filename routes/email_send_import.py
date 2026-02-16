# routes/email_send_import.py

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
    return mysql.connector.connect(
        host=Config.TRACK_DB_HOST,
        user=Config.TRACK_DB_USER,
        password=Config.TRACK_DB_PASS,
        database=Config.TRACK_DB_NAME,
        port=Config.TRACK_DB_PORT,
        use_pure=True,
    )


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


# ✅ DEDUPE IDENTITY (UPDATED):
# Only sender + receiver + sent_at (subject/body/responds can change and will UPDATE)
def make_dedupe_key(sender: str, receiver: str, sent_at: datetime) -> str | None:
    if not sent_at:
        return None  # cannot dedupe safely without sent_at

    sent = sent_at.strftime("%Y-%m-%d %H:%M:%S")
    raw = "|".join(
        [
            (sender or "").strip().lower(),
            (receiver or "").strip().lower(),
            sent,
        ]
    )
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


# =========================
# Upload API
# =========================
@email_send_import_bp.route("/upload", methods=["POST"])
def upload_email_send_file():
    """
    POST multipart/form-data
      - file: .xlsx/.xls/.csv

    Requires DB change:
      email_send_logs must include:
        - dedupe_key VARCHAR(32)
        - UNIQUE INDEX on dedupe_key

    Behavior:
      - Identity = sender + receiver + sent_at
      - If same identity exists:
          - If any allowed fields changed -> UPDATE
          - Else -> count as duplicate (no update)
      - If not exists -> INSERT
      - Unsubscribe override:
          if email_subscription_preferences.is_subscribed = 0
          -> Responds='Unsubscribed'
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

        df.columns = [normalize_header(c) for c in df.columns]

        missing = [h for h in EXPECTED_HEADERS if h not in df.columns]
        extra = [c for c in df.columns if c not in EXPECTED_HEADERS]
        if missing:
            return api_response(
                "Invalid file headers",
                400,
                {"missing_headers": missing, "extra_headers": extra, "expected": EXPECTED_HEADERS},
            )

        df = df[EXPECTED_HEADERS].copy().fillna("")

        # --------------------
        # DB connect
        # --------------------
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)

        skipped = 0
        inserted = 0
        updated = 0
        duplicates_no_change = 0
        unsubscribed_override_count = 0

        # --------------------
        # Build candidates + dedupe keys
        # --------------------
        candidates = []
        dedupe_keys = []

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

            # ✅ Need sent_at for stable dedupe
            dkey = make_dedupe_key(sender_email, receiver_email, sent_at)
            if not dkey:
                skipped += 1
                continue

            # ✅ Unsubscribe override
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

            candidates.append(
                {
                    "dedupe_key": dkey,
                    "sender_email": sender_email,
                    "receiver_email": receiver_email,
                    "first_name": first_name,
                    "company": company,
                    "status": status,
                    "status_message": status_message,
                    "sent_at": sent_at,
                    "responds": responds_value,
                    "subject": subject,
                    "body": body,
                }
            )
            dedupe_keys.append(dkey)

        if not candidates:
            return api_response(
                "No valid rows found to insert/update (SentAt missing?)",
                400,
                {"skipped": skipped},
            )

        # --------------------
        # Fetch existing rows by dedupe_key (chunked)
        # --------------------
        existing_map = {}  # dedupe_key -> existing row
        CHUNK = 500

        for i in range(0, len(dedupe_keys), CHUNK):
            chunk = dedupe_keys[i : i + CHUNK]
            placeholders = ",".join(["%s"] * len(chunk))
            cur.execute(
                f"""
                SELECT id, dedupe_key, first_name, company, responds, status, status_message, subject, body
                FROM email_send_logs
                WHERE dedupe_key IN ({placeholders})
                """,
                chunk,
            )
            for row in cur.fetchall():
                existing_map[row["dedupe_key"]] = row

        # --------------------
        # Decide insert vs update vs duplicate(no change)
        # --------------------
        insert_rows = []
        update_rows = []

        def same(a, b):
            return (a or None) == (b or None)

        for c in candidates:
            ex = existing_map.get(c["dedupe_key"])

            if not ex:
                insert_rows.append(
                    (
                        c["sender_email"],
                        c["receiver_email"],
                        c["first_name"],
                        c["company"],
                        c["status"],
                        c["status_message"],
                        c["sent_at"],
                        c["responds"],
                        c["subject"],
                        c["body"],
                        c["dedupe_key"],
                    )
                )
                continue

            # ✅ Allowed updates: responds + subject + body + status + status_message (+ optional first_name/company)
            changed = False

            if not same(ex.get("responds"), c["responds"]):
                changed = True
            if not same(ex.get("status"), c["status"]):
                changed = True
            if not same(ex.get("status_message"), c["status_message"]):
                changed = True
            if not same(ex.get("subject"), c["subject"]):
                changed = True
            if not same(ex.get("body"), c["body"]):
                changed = True

            # Optional: also update these if you want
            if not same(ex.get("first_name"), c["first_name"]):
                changed = True
            if not same(ex.get("company"), c["company"]):
                changed = True

            if not changed:
                duplicates_no_change += 1
                continue

            update_rows.append(
                (
                    c["first_name"],
                    c["company"],
                    c["status"],
                    c["status_message"],
                    c["responds"],
                    c["subject"],
                    c["body"],
                    int(ex["id"]),
                )
            )

        # --------------------
        # Execute DB writes
        # --------------------
        if insert_rows:
            cur.executemany(
                """
                INSERT INTO email_send_logs
                (sender_email, receiver_email, first_name, company, status, status_message,
                 sent_at, responds, subject, body, dedupe_key)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                    subject=%s,
                    body=%s
                WHERE id=%s
                """,
                update_rows,
            )
            updated = cur.rowcount

        conn.commit()

        return api_response(
            "Imported successfully (dedupe=sender+receiver+sent_at, selective update)",
            200,
            {
                "inserted": inserted,
                "updated": updated,
                "duplicates_no_change": duplicates_no_change,
                "skipped": skipped,
                "total_rows_in_file": int(len(df)),
                "unsubscribed_overrides": unsubscribed_override_count,
            },
        )

    except mysql.connector.IntegrityError as ie:
        # In case unique index exists and concurrent insert happens
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
# List API
# =========================
@email_send_import_bp.route("/list", methods=["POST"])
def list_email_logs():
    data = request.get_json(silent=True) or {}

    page = int(data.get("page", 1))
    per_page = int(data.get("per_page", 10))
    date_val = (data.get("date") or "").strip()  # single date: YYYY-MM-DD
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

        count_sql = f"SELECT COUNT(*) AS total FROM email_send_logs {where_sql}"
        cur.execute(count_sql, params)
        total_records = int(cur.fetchone()["total"])

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
