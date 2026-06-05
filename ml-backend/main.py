import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
import pandas as pd
from surprise import Reader, Dataset, SVD
from dotenv import load_dotenv

# Load environment variables from root
load_dotenv(dotenv_path="../.env")

GLOBAL_MODEL = None
ALL_PRODUCT_IDS = []


def train_recommendation_model():
    global GLOBAL_MODEL, ALL_PRODUCT_IDS
    print("Connecting to MongoDB and fetching optimized ratings pipeline...")
    
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        raise ValueError("MONGO_URI environment variable is missing!")
        
    client = MongoClient(mongo_uri)
    
    # Safely extract the default database baked into your URI ('ecommerce')
    db = client.get_default_database()
    
    # If the URI doesn't explicitly name a database, connect to 'ecommerce' manually
    if db is None:
        db = client["ecommerce"]
    
    # 1. Gather all product IDs for our prediction list
    # Doing this with a lightweight distinct call keeps our memory trace tiny
    ALL_PRODUCT_IDS = [str(pid) for pid in db["products"].distinct("_id")]

    # 2. Define our high-performance Aggregation Pipeline
    pipeline = [
        # Stage 1: Skip products that have no reviews to save processing power
        {"$match": {"reviews": {"$exists": True, "$not": {"$size": 0}}}},
        
        # Stage 2: Shatter the reviews array into individual flat documents
        {"$unwind": "$reviews"},
        
        # Stage 3: Project exactly what SVD needs and drop everything else
        {
            "$project": {
                "_id": 0,
                "item": {"$toString": "$_id"},
                "user": {"$toString": "$reviews.user"},
                "rating": "$reviews.rating"
            }
        }
    ]
    
    # 3. Execute aggregation and immediately cast it to a list of dicts
    print("Executing MongoDB aggregation pipeline...")
    ratings_data = list(db["products"].aggregate(pipeline))
    client.close()
    
    if len(ratings_data) < 5:
        print("Not enough rating data found in MongoDB. SVD training skipped.")
        return
        
    # CRITICAL ADVANTAGE: ratings_data is already a flat list of dicts!
    # No more nested for loops in Python!
    df = pd.DataFrame(ratings_data)
    
    print(f"Extracted {len(ratings_data)} flat ratings. Training SVD Engine...")
    reader = Reader(rating_scale=(1, 5))
    data = Dataset.load_from_df(df[["user", "item", "rating"]], reader)
    trainset = data.build_full_trainset()
    
    print("Training SVD Matrix Factorization model...")
    model = SVD()
    model.fit(trainset)
    
    GLOBAL_MODEL = model
    print("ML Model updated successfully and stored in memory!")

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        train_recommendation_model()
    except Exception as e:
        print(f"❌ Failed to train model on startup: {str(e)}")
    yield
    print("Shutting down ML Recommendation Service...")


app = FastAPI(title="ML Recommendation Service", lifespan=lifespan)

@app.get("/health")
def health_check():
    return {"status": "healthy", "model_loaded": GLOBAL_MODEL is not None}

@app.get("/retrain")
def retrain_model():
    try:
        train_recommendation_model()
        return {"message": "Model retrained successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/recommend/{user_id}")
def get_recommendations(user_id: str):
    if GLOBAL_MODEL is None:
        raise HTTPException(status_code=503, detail="Model is not trained or loaded yet.")
        
    try:
        # Predict ratings for all items the user hasn't interacted with
        predictions = []
        for item_id in ALL_PRODUCT_IDS:
            pred = GLOBAL_MODEL.predict(user_id, item_id)
            predictions.append((item_id, pred.est))
            
        # Sort by predicted rating in descending order
        predictions.sort(key=lambda x: x[1], reverse=True)
        
        # Take the top 10 recommended product IDs
        top_10_ids = [item_id for item_id, score in predictions[:10]]
        
        return {"user_id": user_id, "recommendations": top_10_ids}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))