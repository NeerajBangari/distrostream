import json
import os
from os import path
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from aiokafka import AIOKafkaProducer
import boto3
from botocore.client import Config
from sqlalchemy.orm import Session

# 👇 IMPORT YOUR DATABASE UTILITIES & MODELS
from app.database import init_db, get_db, VideoMetadata

app = FastAPI(title="DistroStream Ingest Service")

origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],  # Allows GET, POST, OPTIONS, etc.
    allow_headers=["*"],  # Allows all authorization/content headers
)

# Infrastructure Constants (Configured for local Docker networking)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_TOPIC = "raw-video-events"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadminpassword")
BUCKET_NAME = "raw-videos"

# Global placeholders for external resource clients
kafka_producer = None
s3_client = None


@app.on_event("startup")
async def startup_event():
    global kafka_producer, s3_client
    
    # 0. Automatically generate/verify PostgreSQL table structures
    init_db()
    print("💾 PostgreSQL tables initialized successfully.")
    
    # 1. Initialize Async Kafka Producer
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await kafka_producer.start()
    print("🚀 Connected successfully to Apache Kafka")
    
    # 2. Initialize MinIO Client using boto3 (S3 SDK)
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4')
    )
    print("📦 Connected successfully to MinIO Storage")


@app.on_event("shutdown")
async def shutdown_event():
    global kafka_producer
    if kafka_producer:
        await kafka_producer.stop()
        print("🛑 Disconnected from Kafka")


@app.get("/api/v1/videos")
def get_all_videos(db: Session = Depends(get_db)):
    """Fetches all video metadata logs from the distributed database."""
    videos = db.query(VideoMetadata).order_by(VideoMetadata.created_at.desc()).all()
    return videos


@app.post("/api/v1/ingest/upload")
async def upload_video(file: UploadFile = File(...), db: Session = Depends(get_db)):
    # 1. Guardrail validation check happens FIRST before touching database state
    if not file.filename.endswith(('.mp4', '.mkv', '.avi')):
        raise HTTPException(status_code=400, detail="Unsupported video format extension.")
        
    # Standardize our target lookup tracking key
    video_id = os.path.splitext(file.filename)[0]

    # 👇 NEW: Check if this video has already been uploaded or processed
    existing_video = db.query(VideoMetadata).filter(VideoMetadata.video_id == video_id).first()
    if existing_video:
        raise HTTPException(
            status_code=409, 
            detail=f"Conflict: A video with the identifier '{video_id}' already exists in your library."
        )

    # 2. Persist the state row tracking record safely inside PostgreSQL
    db_video = VideoMetadata(video_id=video_id, status="PROCESSING")
    db.add(db_video)
    try:
        db.commit()
        print(f"📝 Persistent track state established for job context: {video_id}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail="Database write concurrency error.")
    
    # Generate a safe object name inside our storage bucket
    object_name = f"raw_{file.filename}"
    
    try:
        # 3. Stream file data directly into MinIO storage
        s3_client.upload_fileobj(file.file, BUCKET_NAME, object_name)
        storage_path = f"minio://{BUCKET_NAME}/{object_name}"
        
        # 4. Package metadata and publish to Kafka asynchronously
        event_payload = {
            "video_id": video_id,
            "filename": file.filename,
            "storage_path": storage_path,
            "status": "pending"
        }
        
        # Send event down the pipeline channel
        await kafka_producer.send_and_wait(KAFKA_TOPIC, event_payload)
        
        # Return immediate structural feedback back to the client UI
        return {
            "status": "Accepted",
            "message": "Video uploaded successfully and queued for processing.",
            "data": event_payload
        }
        
    except Exception as e:
        print(f"❌ Error handling ingestion: {str(e)}")
        # Fail-safe database cleanup tracking hook
        db_video.status = "FAILED"
        db.commit()
        raise HTTPException(status_code=500, detail=f"Ingestion pipeline failure: {str(e)}")