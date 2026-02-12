from flask import Blueprint, request, Response
from datetime import datetime
import base64
import mysql.connector
from config import Config

email_tracking_bp = Blueprint("email_tracking", __name__)

# 1x1 transparent GIF
GIF_BASE64 = "R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw=="
PIXEL_BYTES = base64.b64decode(GIF_BASE64)


def get_tracking_db():
    return mysql.connector.connect(
        host=Config.TRACK_DB_HOST,
        user=Config.TRACK_DB_USER,
        password=Config.TRACK_DB_PASS,
        database=Config.TRACK_DB_NAME,
        port=Config.TRACK_DB_PORT,
    )


def norm_email(val: str) -> str:
    return (val or "").strip().lower()


def _pixel_response():
    resp = Response(PIXEL_BYTES, mimetype="image/gif")
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


def _now_epoch():
    return int(datetime.now().timestamp())


def _parse_epoch(val):
    try:
        if not val:
            return 0
        return int(float(str(val).strip()))
    except Exception:
        return 0


# =========================================================
# OPEN TRACKING
# =========================================================
@email_tracking_bp.route("/open.gif", methods=["GET"])
def track_open():
    send_key = (request.args.get("k") or "").strip()
    st_epoch = _parse_epoch(request.args.get("st"))

    receiver = norm_email(request.args.get("to", ""))
    sender = norm_email(request.args.get("from", ""))

    if not send_key:
        return _pixel_response()

    if st_epoch <= 0:
        return _pixel_response()

    if _now_epoch() < (st_epoch + 60):
        return _pixel_response()

    try:
        conn = get_tracking_db()
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO email_open_events
              (sender_email, receiver_email, send_key, opened_at)
            VALUES (%s, %s, %s, %s)
            """,
            (sender, receiver, send_key, datetime.now()),
        )

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print("Open tracking error:", e)

    return _pixel_response()


# =========================================================
# UNSUBSCRIBE
# =========================================================
@email_tracking_bp.route("/unsub", methods=["GET", "POST"])
def unsubscribe():
    args = request.args if request.method == "GET" else request.form

    receiver = norm_email(args.get("to") or args.get("email"))
    sender = norm_email(args.get("from") or args.get("sender"))

    if sender and receiver:
        try:
            conn = get_tracking_db()
            cur = conn.cursor()

            cur.execute(
                """
                INSERT INTO email_subscription_preferences
                  (sender_email, receiver_email, is_subscribed, updated_at)
                VALUES (%s, %s, 0, %s)
                ON DUPLICATE KEY UPDATE
                  is_subscribed=0,
                  updated_at=VALUES(updated_at)
                """,
                (sender, receiver, datetime.now()),
            )

            conn.commit()
            cur.close()
            conn.close()

        except Exception as e:
            print("Unsubscribe error:", e)

    return Response("unsubscribed", mimetype="text/plain")

