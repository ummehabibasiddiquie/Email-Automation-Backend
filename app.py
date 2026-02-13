from flask import Flask, jsonify
from routes.email_tracking import email_tracking_bp

app = Flask(__name__)

# IMPORTANT
app.register_blueprint(email_tracking_bp, url_prefix="/email/email_tracking")

# print("\n==== REGISTERED ROUTES ====")
# for r in app.url_map.iter_rules():
#     print(r, r.methods)
# print("==== END ROUTES ====\n")

@app.get("/email/")
def root():
    return jsonify({"service": "email-tracking-backend", "status": "running"})

# @app.get("/health")
# def health():
#     return jsonify({"ok": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
