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

def _esc_html(s: str) -> str:
    # minimal HTML escaping (prevents HTML injection in confirm page)
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

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
# UNSUBSCRIBE
# =========================================================
@email_tracking_bp.route("/unsub", methods=["GET", "POST"])
def unsubscribe():
    """
    Scanner-safe unsubscribe:
    - GET: show confirmation page only (no DB write)
    - POST: perform DB update (real user confirmation)
    """
    ip = _client_ip()
    ua = request.headers.get("User-Agent", "")
    method = request.method

    # -----------------------
    # GET: confirmation page
    # -----------------------
    if method == "GET":
        send_key = (request.args.get("k") or "").strip()
        receiver = norm_email(request.args.get("to") or request.args.get("email"))
        sender = norm_email(request.args.get("from") or request.args.get("sender"))

        print("[unsub][GET]", "ip=", ip, "ua=", ua, "k=", send_key, "from=", sender, "to=", receiver, flush=True)

        if not send_key:
            return Response("Invalid unsubscribe link (missing k).", status=400, mimetype="text/plain")

        # IMPORTANT: do NOT update DB on GET
        return Response(_confirm_page_html(send_key, sender, receiver), mimetype="text/html")

    # -----------------------
    # POST: actual unsubscribe
    # -----------------------
    send_key = (request.form.get("k") or "").strip()
    receiver = norm_email(request.form.get("to") or request.form.get("email"))
    sender = norm_email(request.form.get("from") or request.form.get("sender"))

    print("[unsub][POST]", "ip=", ip, "ua=", ua, "k=", send_key, "from=", sender, "to=", receiver, flush=True)

    if not (sender and receiver):
        return Response("Invalid unsubscribe request (missing sender/receiver).", status=400, mimetype="text/plain")

    conn = None
    cur = None
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
        return Response("You have been unsubscribed.", mimetype="text/plain")

    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        print("[unsub][ERROR]", repr(e), flush=True)
        return Response("Unsubscribe failed. Please try again later.", status=500, mimetype="text/plain")

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