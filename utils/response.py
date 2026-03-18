# utils/response.py
from flask import jsonify

def api_response(status: int, message: str, data=None):
    payload = {
        "status": status,
        "message": message,
        "data": data or {}
    }
    return jsonify(payload), status
