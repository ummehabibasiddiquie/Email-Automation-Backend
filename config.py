import os
from dotenv import load_dotenv
import mysql.connector

load_dotenv()

class Config:
    TRACK_DB_HOST = os.getenv("TRACK_DB_HOST")
    TRACK_DB_USER = os.getenv("TRACK_DB_USER")
    TRACK_DB_PASS = os.getenv("TRACK_DB_PASS")
    TRACK_DB_NAME = os.getenv("TRACK_DB_NAME")
    TRACK_DB_PORT = int(os.getenv("TRACK_DB_PORT", "3306"))


def get_db_connection():
    return mysql.connector.connect(
        host=Config.TRACK_DB_HOST,
        user=Config.TRACK_DB_USER,
        password=Config.TRACK_DB_PASS,
        database=Config.TRACK_DB_NAME,
        port=Config.TRACK_DB_PORT
    )