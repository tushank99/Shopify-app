import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
import pandas as pd
from surprise import Reader, Dataset, SVD
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import List
from apscheduler.schedulers.background import BackgroundScheduler

# Load environment variables from root
load_dotenv(dotenv_path="../.env")

GLOBAL_MODEL = None
GLOBAL_DF = None 
ALL_PRODUCT_IDS = []

class MatrixUpdateRequest(BaseModel):
    userId: str
    productIds: List[str]
    eventType: str

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

    ALL_PRODUCT_IDS = [str(pid) for pid in db["products"].distinct("_id")]

    # High-performance Aggregation Pipeline
    pipeline = [
        {"$match": {"reviews": {"$exists": True, "$not": {"$size": 0}}}},
        {"$unwind": "$reviews"},
        {
            "$project": {
                "_id": 0,
                "item": {"$toString": "$_id"},
                "user": {"$toString": "$reviews.user"},
                "rating": "$reviews.rating"
            }
        }
    ]

    print("Executing MongoDB aggregation pipeline...")
    ratings_data = list(db["products"].aggregate(pipeline))
    client.close()

    if len(ratings_data) < 5:
        print("Not enough rating data found in MongoDB. SVD training skipped.")
        return

    GLOBAL_DF = pd.DataFrame(ratings_data)
    print(f"Extracted {len(ratings_data)} flat ratings. Training SVD Engine...")
    
    reader = Reader(rating_scale=(1, 5))
    data = Dataset.load_from_df(GLOBAL_DF[["user", "item", "rating"]], reader)
    trainset = data.build_full_trainset()
    
    print("Training SVD Matrix Factorization model...")
    model = SVD()
    model.fit(trainset)
    GLOBAL_MODEL = model
    print("ML Model updated successfully and stored in memory!")


#  (Called natively within Python process)
def scheduled_retrain_task():
    global GLOBAL_MODEL, GLOBAL_DF
    
    if GLOBAL_DF is None:
        print("⏰ [INTERNAL CRON] Dataframe uninitialized. Skipping routine matrix refactor.")
        return
        
    print("[INTERNAL CRON] Scheduled window hit! Recalculating SVD matrix latent features natively...")
    try:
        reader = Reader(rating_scale=(1, 5))
        data = Dataset.load_from_df(GLOBAL_DF[['user', 'item', 'rating']], reader)
        trainset = data.build_full_trainset()
        
        model = SVD()
        model.fit(trainset)
        GLOBAL_MODEL = model
        print(f" [INTERNAL CRON] SVD Matrix successfully self-refactored. Rows handled: {len(GLOBAL_DF)}")
    except Exception as e:
        print(f"[INTERNAL CRON] Scheduled matrix compilation failed: {str(e)}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Train model baseline on startup
    try:
        train_recommendation_model()
    except Exception as e:
        print(f"Failed to train model on startup: {str(e)}")
        
    # 2.  Initialize and Start the Native Background MLOps Scheduler
    scheduler = BackgroundScheduler()
    
    # Task A: Production Rule - Run everyday at 3:00 AM off-peak hours
    scheduler.add_job(scheduled_retrain_task, 'cron', hour=3, minute=0)
    
    # Task B: Presentation Demo Rule - Run every 2 minutes automatically for live tracing
    scheduler.add_job(scheduled_retrain_task, 'interval', minutes=2)
    
    scheduler.start()
    print(" Python MLOps Background Daemon active and monitoring scheduling registers...")
    
    yield
    
    # 3. Shutdown scheduler threads gracefully when FastAPI stops
    print("Shutting down MLOps Background Daemon...")
    scheduler.shutdown()
    print("Shutting down ML Recommendation Service...")


app = FastAPI(title="ML Recommendation Service", lifespan=lifespan)

@app.get("/health")
def health_check():
    return {"status": "healthy", "model_loaded": GLOBAL_MODEL is not None}


# Keep the explicit POST route open for manual presentation overrides/testing
@app.post("/retrain")
async def force_retrain():
    scheduled_retrain_task()
    return {
        "status": "success", 
        "message": "SVD Matrix manually refactored via override API.",
        "total_rows_trained": len(GLOBAL_DF) if GLOBAL_DF is not None else 0
    }


# Processes streaming BullMQ checkout events
@app.post("/update-matrix")
async def update_matrix(request: MatrixUpdateRequest):
    global GLOBAL_DF
    
    if GLOBAL_DF is None:
        raise HTTPException(status_code=503, detail="Interaction matrix dataframe is not initialized yet.")
        
    if request.eventType == "PURCHASE":
        new_interactions = []
        for product_id in request.productIds:
            new_interactions.append({
                "user": str(request.userId),
                "item": str(product_id),
                "rating": 5.0
            })
            
        new_df = pd.DataFrame(new_interactions)
        GLOBAL_DF = pd.concat([GLOBAL_DF, new_df], ignore_index=True)
        
        print(f"Injected {len(request.productIds)} high-weight vectors for user {request.userId}")
        print(f"Total in-memory training rows available for next batch re-train: {len(GLOBAL_DF)}")
        
    return {"status": "success", "message": "Matrix interaction weights updated."}


@app.get("/recommend/{user_id}")
def get_recommendations(user_id: str):
    if GLOBAL_MODEL is None:
        raise HTTPException(status_code=503, detail="Model is not trained or loaded yet.")
        
    try:
        predictions = []
        for item_id in ALL_PRODUCT_IDS:
            pred = GLOBAL_MODEL.predict(user_id, item_id)
            predictions.append((item_id, pred.est))
            
        predictions.sort(key=lambda x: x[1], reverse=True)
        top_10_ids = [item_id for item_id, score in predictions[:10]]
        return {"user_id": user_id, "recommendations": top_10_ids}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))