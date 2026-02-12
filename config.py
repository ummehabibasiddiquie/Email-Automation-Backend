import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    TRACK_DB_HOST = os.getenv("TRACK_DB_HOST")
    TRACK_DB_USER = os.getenv("TRACK_DB_USER")
    TRACK_DB_PASS = os.getenv("TRACK_DB_PASS")
    TRACK_DB_NAME = os.getenv("TRACK_DB_NAME")
    TRACK_DB_PORT = int(os.getenv("TRACK_DB_PORT", "3306"))
