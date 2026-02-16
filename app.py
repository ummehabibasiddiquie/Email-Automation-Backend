from flask import Flask, jsonify
from routes.email_tracking import email_tracking_bp
from routes.email_send_import import email_send_import_bp
from flask_cors import CORS
import os

app = Flask(__name__)

# IMPORTANT
app.register_blueprint(email_tracking_bp, url_prefix="/email_tracking")
app.register_blueprint(email_send_import_bp, url_prefix="/email_send_import")

# print("\n==== REGISTERED ROUTES ====")
# for r in app.url_map.iter_rules():
#     print(r, r.methods)
# print("==== END ROUTES ====\n")

# CORS(app, supports_credentials=True)
# CORS(app, resources={r"/*": {"origins": "*"}})
CORS(app, resources={r"/*": {"origins": ["https://email-automation-dashboard-maox.vercel.app", "https://email-file-sending.vercel.app"]}})

@app.get("/")
def root():
    return jsonify({"service": "email-tracking-backend", "status": "running"})

# @app.get("/health")
# def health():
#     return jsonify({"ok": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
