import os
import threading
from contextlib import asynccontextmanager
from collections import OrderedDict
from typing import List, Optional

import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
from surprise import Dataset, Reader, SVD

# Load environment variables from root
load_dotenv(dotenv_path="../.env")

GLOBAL_MODEL = None
GLOBAL_DF = None
ALL_PRODUCT_IDS = []
STATE_LOCK = threading.Lock()

# Bounded memory of already-applied order ids so retried/duplicate events are ignored
PROCESSED_ORDER_IDS = OrderedDict()
MAX_TRACKED_ORDER_IDS = 50000


class MatrixUpdateRequest(BaseModel):
    userId: str
    productIds: List[str]
    eventType: str
    orderId: Optional[str] = None


def train_recommendation_model():
    global GLOBAL_MODEL, ALL_PRODUCT_IDS, GLOBAL_DF
    print("Connecting to MongoDB and fetching optimized ratings pipeline...")

    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        raise ValueError("MONGO_URI environment variable is missing!")

    client = MongoClient(mongo_uri)
    db = client.get_default_database()
    if db is None:
        db = client["ecommerce"]

    product_ids = [str(pid) for pid in db["products"].distinct("_id")]

    pipeline = [
        {"$match": {"reviews": {"$exists": True, "$not": {"$size": 0}}}},
        {"$unwind": "$reviews"},
        {
            "$project": {
                "_id": 0,
                "item": {"$toString": "$_id"},
                "user": {"$toString": "$reviews.user"},
                "rating": "$reviews.rating",
            }
        },
    ]

    print("Executing MongoDB aggregation pipeline...")
    ratings_data = list(db["products"].aggregate(pipeline))
    client.close()

    if len(ratings_data) < 5:
        with STATE_LOCK:
            ALL_PRODUCT_IDS = product_ids
            GLOBAL_DF = pd.DataFrame(columns=["user", "item", "rating"])
        print("Not enough rating data found in MongoDB. SVD training skipped.")
        return

    df_snapshot = pd.DataFrame(ratings_data)
    print(f"Extracted {len(ratings_data)} flat ratings. Training SVD Engine...")

    reader = Reader(rating_scale=(1, 5))
    data = Dataset.load_from_df(df_snapshot[["user", "item", "rating"]], reader)
    trainset = data.build_full_trainset()

    print("Training SVD Matrix Factorization model...")
    model = SVD()
    model.fit(trainset)

    with STATE_LOCK:
        ALL_PRODUCT_IDS = product_ids
        GLOBAL_DF = df_snapshot
        GLOBAL_MODEL = model

    print("ML Model updated successfully and stored in memory!")


def scheduled_retrain_task():
    global GLOBAL_MODEL, GLOBAL_DF
    with STATE_LOCK:
        if GLOBAL_DF is None or len(GLOBAL_DF) < 5:
            print("⏰ [INTERNAL CRON] Dataframe uninitialized or too small. Skipping routine matrix refactor.")
            return
        df_snapshot = GLOBAL_DF[["user", "item", "rating"]].copy()

    print("[INTERNAL CRON] Scheduled window hit! Recalculating SVD matrix latent features natively...")
    try:
        reader = Reader(rating_scale=(1, 5))
        data = Dataset.load_from_df(df_snapshot, reader)
        trainset = data.build_full_trainset()
        model = SVD()
        model.fit(trainset)
        with STATE_LOCK:
            GLOBAL_MODEL = model
        print(f"[INTERNAL CRON] SVD Matrix successfully self-refactored. Rows handled: {len(df_snapshot)}")
    except Exception as e:
        print(f"[INTERNAL CRON] Scheduled matrix compilation failed: {str(e)}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        train_recommendation_model()
    except Exception as e:
        print(f"Failed to train model on startup: {str(e)}")

    scheduler = BackgroundScheduler()
    scheduler.add_job(scheduled_retrain_task, "cron", hour=3, minute=0)
    scheduler.add_job(scheduled_retrain_task, "interval", minutes=2)
    scheduler.start()
    print("Python MLOps Background Daemon active and monitoring scheduling registers...")
    yield
    print("Shutting down MLOps Background Daemon...")
    scheduler.shutdown()
    print("Shutting down ML Recommendation Service...")


app = FastAPI(title="ML Recommendation Service", lifespan=lifespan)


@app.get("/health")
def health_check():
    return {"status": "healthy", "model_loaded": GLOBAL_MODEL is not None}


@app.post("/retrain")
def force_retrain():
    scheduled_retrain_task()
    with STATE_LOCK:
        total_rows = len(GLOBAL_DF) if GLOBAL_DF is not None else 0
    return {
        "status": "success",
        "message": "SVD Matrix manually refactored via override API.",
        "total_rows_trained": total_rows,
    }


@app.post("/update-matrix")
def update_matrix(request: MatrixUpdateRequest):
    global GLOBAL_DF
    total_rows = 0

    with STATE_LOCK:
        if GLOBAL_DF is None:
            raise HTTPException(
                status_code=503,
                detail="Interaction matrix dataframe is not initialized yet.",
            )

        if request.eventType == "PURCHASE":
            if request.orderId and request.orderId in PROCESSED_ORDER_IDS:
                return {"status": "success", "message": "Duplicate order event ignored."}

            if not request.productIds:
                return {"status": "success", "message": "No product IDs to update."}

            new_interactions = [
                {"user": str(request.userId), "item": str(product_id), "rating": 5.0}
                for product_id in request.productIds
            ]
            new_df = pd.DataFrame(new_interactions)
            GLOBAL_DF = pd.concat([GLOBAL_DF, new_df], ignore_index=True)

            if request.orderId:
                PROCESSED_ORDER_IDS[request.orderId] = True
                while len(PROCESSED_ORDER_IDS) > MAX_TRACKED_ORDER_IDS:
                    PROCESSED_ORDER_IDS.popitem(last=False)

        total_rows = len(GLOBAL_DF)

    if request.eventType == "PURCHASE":
        print(f"Injected {len(request.productIds)} high-weight vectors for user {request.userId}")
        print(f"Total in-memory training rows available for next batch re-train: {total_rows}")

    return {"status": "success", "message": "Matrix interaction weights updated."}


@app.get("/recommend/{user_id}")
def get_recommendations(user_id: str):
    with STATE_LOCK:
        model = GLOBAL_MODEL
        product_ids = list(ALL_PRODUCT_IDS)

    if model is None:
        raise HTTPException(status_code=503, detail="Model is not trained or loaded yet.")

    try:
        predictions = []
        for item_id in product_ids:
            pred = model.predict(user_id, item_id)
            predictions.append((item_id, pred.est))

        predictions.sort(key=lambda x: x[1], reverse=True)
        top_10_ids = [item_id for item_id, _ in predictions[:10]]
        return {"user_id": user_id, "recommendations": top_10_ids}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))