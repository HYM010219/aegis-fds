import os
from pymongo import AsyncMongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/?directConnection=true")
MONGO_DB = os.getenv("MONGO_DB", "aegis")

mongo_client = AsyncMongoClient(MONGO_URI, serverSelectionTimeoutMS=3000, tz_aware=True)
db = mongo_client[MONGO_DB]