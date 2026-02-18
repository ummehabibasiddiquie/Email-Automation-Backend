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


def api_response(message, status=200, data=None):
    return {"message": message, "status": status, "data": data or {}}, status


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


def _esc_html(s: str) -> str:
    # minimal HTML escaping (prevents HTML injection in confirm page)
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _client_ip() -> str:
    # if behind nginx, X-Forwarded-For has real IP
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.remote_addr or ""


def _confirm_page_html(send_key: str, sender: str, receiver: str) -> str:
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Unsubscribe</title>
</head>
<body style="font-family:Arial,sans-serif;background:#f6f7fb;margin:0;padding:24px;">
  <div style="max-width:520px;margin:0 auto;background:#fff;border:1px solid #eee;border-radius:12px;padding:20px;">
    <h2 style="margin:0 0 10px;">Confirm unsubscribe</h2>
    <p style="margin:0 0 14px;color:#444;line-height:1.5;">
      Are you sure you want to unsubscribe from future emails?
    </p>

    <div style="margin:0 0 14px;font-size:12px;color:#666;line-height:1.4;">
      <div><b>Sender:</b> {_esc_html(sender) if sender else "-"}</div>
      <div><b>Receiver:</b> {_esc_html(receiver) if receiver else "-"}</div>
    </div>

    <form method="POST" action="/email/email_tracking/unsub" style="margin:0;">
      <input type="hidden" name="k" value="{_esc_html(send_key)}">
      <input type="hidden" name="from" value="{_esc_html(sender)}">
      <input type="hidden" name="to" value="{_esc_html(receiver)}">

      <button type="submit"
        style="background:#d92d20;color:#fff;border:none;padding:10px 14px;border-radius:10px;cursor:pointer;font-weight:600;">
        Yes, unsubscribe
      </button>

      <a href="https://tfshrms.cloud"
         style="margin-left:12px;color:#555;text-decoration:none;">
        Cancel
      </a>
    </form>

    <p style="margin:16px 0 0;color:#777;font-size:12px;">
      If you didn’t request this, you can safely close this page.
    </p>
  </div>
</body>
</html>"""


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
# UNSUBSCRIBE (POST ONLY)
# =========================================================
@email_tracking_bp.route("/unsub", methods=["POST"])
def unsubscribe():
    """
    Frontend calls POST with JSON:
      { "from": "...", "to": "...", "k": "optional" }

    We upsert into email_subscription_preferences:
      is_subscribed = 0
      updated_at = NOW

    ✅ NEW:
    Also update email_send_logs (NO INSERT, NO DUP ROWS)
    Set responds='UNSUBSCRIBED' for the latest SENT+NOT_RESPONDED row for sender/receiver
    """

    payload = request.get_json(silent=True) or {}

    sender = norm_email(
        payload.get("from")
        or payload.get("sender")
        or request.form.get("from")
        or request.form.get("sender")
    )

    receiver = norm_email(
        payload.get("to")
        or payload.get("email")
        or request.form.get("to")
        or request.form.get("email")
    )

    send_key = (payload.get("k") or request.form.get("k") or "").strip()  # optional

    if not sender or not receiver:
        return api_response("Missing sender/receiver", 400)

    conn = None
    cur = None
    try:
        conn = get_tracking_db()
        cur = conn.cursor()
        now_dt = datetime.now()

        # 1) Upsert subscription preference
        cur.execute(
            """
            INSERT INTO email_subscription_preferences
              (sender_email, receiver_email, is_subscribed, updated_at)
            VALUES (%s, %s, 0, %s)
            ON DUPLICATE KEY UPDATE
              is_subscribed=0,
              updated_at=VALUES(updated_at)
            """,
            (sender, receiver, now_dt),
        )

        # 2) Update latest SENT + NOT_RESPONDED log row (no insert)
        cur.execute(
            """
            UPDATE email_send_logs
            SET responds=%s,
                status_message=%s,
                updated_at=%s
            WHERE id = (
                SELECT id FROM (
                    SELECT id
                    FROM email_send_logs
                    WHERE sender_email=%s
                      AND receiver_email=%s
                      AND status='SENT'
                      AND (responds IS NULL OR responds='' OR responds='No Response Yet')
                    ORDER BY sent_at DESC, id DESC
                    LIMIT 1
                ) x
            )
            """,
            (
                "UNSUBSCRIBED",
                "Receiver unsubscribed via link",
                now_dt,
                sender,
                receiver,
            ),
        )

        conn.commit()

        return api_response(
            "Unsubscribed successfully",
            200,
            {
                "sender_email": sender,
                "receiver_email": receiver,
                "is_subscribed": 0,
                "k": send_key or None,
            },
        )

    except Exception as e:
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
        return api_response(f"Unsubscribe failed: {str(e)}", 500)

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
