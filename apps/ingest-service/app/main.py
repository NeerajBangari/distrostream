import json
import os
from fastapi import FastAPI, UploadFile, File, HTTPException
from aiokafka import AIOKafkaProducer
import boto3
from botocore.client import Config

app = FastAPI(title="DistroStream Ingest Service")

# Infrastructure Constants (Configured for local Docker networking)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_TOPIC = "raw-video-events"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadminpassword")
BUCKET_NAME = "raw-videos"

# Global placeholdes for external resource clients
kafka_producer = None
s3_client = None

@app.on_event("startup")
async def startup_event():
    global kafka_producer, s3_client
    
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


@app.post("/api/v1/ingest/upload")
async def upload_video(file: UploadFile = File(...)):
    # Simple guardrail validation
    if not file.filename.endswith(('.mp4', '.mkv', '.avi')):
        raise HTTPException(status_code=400, detail="Unsupported video format extension.")
    
    # Generate a safe object name inside our storage bucket
    object_name = f"raw_{file.filename}"
    
    try:
        # 1. Stream the file contents directly out of the memory buffer into local MinIO storage
        # file.file is an underlying standard python SpooledTemporaryFile object
        s3_client.upload_fileobj(file.file, BUCKET_NAME, object_name)
        storage_path = f"minio://{BUCKET_NAME}/{object_name}"
        
        # 2. Package metadata and publish to Kafka asynchronously
        event_payload = {
            "video_id": object_name.replace("raw_", "").split(".")[0],
            "filename": file.filename,
            "storage_path": storage_path,
            "status": "pending"
        }
        
        # Send event to pipeline log
        await kafka_producer.send_and_wait(KAFKA_TOPIC, event_payload)
        
        # Return immediate feedback to the client under 100ms
        return {
            "status": "Accepted",
            "message": "Video uploaded successfully and queued for processing.",
            "data": event_payload
        }
        
    except Exception as e:
        print(f"Error handling ingestion: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ingestion pipeline failure: {str(e)}")