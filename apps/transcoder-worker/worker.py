import asyncio
import json
import os
import shutil
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import boto3
from botocore.client import Config

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
CONSUME_TOPIC = "raw-video-events"
PRODUCE_TOPIC = "processed-stream"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadminpassword")

RAW_BUCKET = "raw-videos"
PROCESSED_BUCKET = "processed-streams"

# Initialize standard synchronous S3 Client (boto3)
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version='s3v4')
)

async def transcode_to_hls(video_id: str, local_input_path: str, output_dir: str):
    """
    Spawns an async FFmpeg subprocess to transcode the input video into HLS format.
    Slices the video into 6-second segments and optimizes it for streaming.
    """
    manifest_path = os.path.join(output_dir, "playlist.m3u8")
    
    # FFmpeg arguments to compile video to HLS 720p output format
    ffmpeg_cmd = [
        "./ffmpeg.exe", "-y", "-i", local_input_path,
        "-profile:v", "baseline", "-level", "3.0",
        "-s", "1280x720", "-start_number", "0",
        "-hls_time", "6", "-hls_list_size", "0",
        "-f", "hls", manifest_path
    ]
    
    print(f"🎬 Running FFmpeg for Video ID: {video_id}...")
    process = await asyncio.create_subprocess_exec(
        *ffmpeg_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    stdout, stderr = await process.communicate()
    
    if process.returncode != 0:
        raise Exception(f"FFmpeg failed: {stderr.decode('utf-8', errors='ignore')}")
    print(f"✅ Transcoding complete for Video ID: {video_id}")
    return manifest_path


async def process_video_event(msg_value, producer):
    """
    Handles the core pipeline logic for a single video event message.
    """
    video_id = msg_value.get("video_id")
    filename = msg_value.get("filename")
    
    # Setup temporary local working directories
    local_raw_path = f"temp_raw_{filename}"
    local_output_dir = f"temp_out_{video_id}"
    os.makedirs(local_output_dir, exist_ok=True)
    
    try:
        # 1. Download raw file from MinIO bucket
        object_key = f"raw_{filename}"
        print(f"📥 Downloading {object_key} from MinIO...")
        s3_client.download_file(RAW_BUCKET, object_key, local_raw_path)
        
        # 2. Run background transcoding process
        await transcode_to_hls(video_id, local_raw_path, local_output_dir)
        
        # 3. Upload all generated HLS chunks (.ts and .m3u8) back to MinIO public bucket
        print(f"📤 Uploading HLS chunks to '{PROCESSED_BUCKET}/{video_id}/'...")
        for file in os.listdir(local_output_dir):
            file_path = os.path.join(local_output_dir, file)
            s3_key = f"{video_id}/{file}"
            s3_client.upload_file(file_path, PROCESSED_BUCKET, s3_key)
            
        # 4. Notify downstream services by publishing an event to the 'processed-stream' topic
        playback_url = f"{MINIO_ENDPOINT}/{PROCESSED_BUCKET}/{video_id}/playlist.m3u8"
        success_payload = {
            "video_id": video_id,
            "status": "completed",
            "playback_url": playback_url
        }
        await producer.send_and_wait(PRODUCE_TOPIC, success_payload)
        print(f"🚀 Published success notification to Kafka for Video ID: {video_id}")
        
    except Exception as e:
        print(f"❌ Pipeline Failure for video {video_id}: {str(e)}")
    finally:
        # Clean up temporary disk storage files
        if os.path.exists(local_raw_path):
            os.remove(local_raw_path)
        if os.path.exists(local_output_dir):
            shutil.rmtree(local_output_dir)


async def main():
    print("🤖 Initializing DistroStream Transcoder Worker...")
    
    # Configure Kafka Consumer & Producer
    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="transcoder-group",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    await consumer.start()
    await producer.start()
    print("🎧 Worker is live and actively listening for raw video events...")
    
    try:
        # Endless event polling loop
        async for msg in consumer:
            print(f"⚡ Received transcoding request event from Kafka!")
            await process_video_event(msg.value, producer)
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())