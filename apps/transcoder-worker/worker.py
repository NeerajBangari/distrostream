import asyncio
import json
import os
import re
import shutil
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import boto3
from botocore.client import Config
from database import VideoMetadata, SessionLocal

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
CONSUME_TOPIC = "raw-video-events"
PRODUCE_TOPIC = "processed-stream"
PROGRESS_TOPIC = "video-progress"  # 👈 New internal pipeline notification channel

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadminpassword")
RAW_BUCKET = "raw-videos"
PROCESSED_BUCKET = "processed-streams"

s3_client = boto3.client(
    's3', endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version='s3v4')
)

def sync_update_db_success(video_id: str, playback_url: str):
    db = SessionLocal()
    try:
        clean_id = os.path.splitext(video_id)[0]
        video_record = db.query(VideoMetadata).filter(VideoMetadata.video_id == clean_id).first()
        if video_record:
            video_record.status = "COMPLETED"
            video_record.playback_url = playback_url
            db.commit()
    finally:
        db.close()

def sync_s3_upload(file_path: str, bucket: str, s3_key: str, content_type: str):
    s3_client.upload_file(file_path, bucket, s3_key, ExtraArgs={'ContentType': content_type})

async def transcode_with_progress(video_id: str, local_input_path: str, output_dir: str, producer: AIOKafkaProducer):
    manifest_path = os.path.join(output_dir, "playlist.m3u8")
    
    # 1. Capture total video duration upfront
    duration_cmd = ["./ffmpeg.exe", "-i", local_input_path]
    p_probe = await asyncio.create_subprocess_exec(*duration_cmd, stderr=asyncio.subprocess.PIPE)
    _, stderr_data = await p_probe.communicate()
    
    duration_text = stderr_data.decode('utf-8', errors='ignore')
    duration_match = re.search(r"Duration:\s+(\d+):(\d+):(\d+)\.(\d+)", duration_text)
    
    total_seconds = 0.0
    if duration_match:
        hours, mins, secs, ms = map(int, duration_match.groups())
        total_seconds = (hours * 3600) + (mins * 60) + secs + (ms / 100)
    
    if total_seconds == 0:
        total_seconds = 10.0  # Fallback baseline
    else:
        print(f"⏳ Target Media File Duration: {total_seconds} seconds")

    # 2. Add the real-time progress engine flags to your array matrix
    ffmpeg_cmd = [
        "./ffmpeg.exe", "-y", 
        "-i", local_input_path,
        "-progress", "pipe:2",  # 👈 CRITICAL: Forces FFmpeg to stream metrics out instantly
        "-profile:v", "baseline", "-level", "3.0",
        "-s", "1280x720", 
        "-start_number", "0",
        "-hls_time", "6", 
        "-hls_list_size", "0",
        "-f", "hls", manifest_path
    ]
    
    print(f"🎬 Spawning Real-time FFmpeg Engine for: {video_id}...")
    process = await asyncio.create_subprocess_exec(
        *ffmpeg_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    # 3. Stream parse the clean out-of-band progress blocks
    while True:
        line_bytes = await process.stderr.readline()
        if not line_bytes:
            break
            
        line = line_bytes.decode('utf-8', errors='ignore').strip()
        
        # When using -progress, FFmpeg outputs clean fields like: "out_time_ms=45000000"
        if "out_time_ms=" in line:
            try:
                # Extract microseconds and convert them to standard seconds
                micro_secs = float(line.split("=")[1])
                current_seconds = micro_secs / 1000000.0
                
                if total_seconds > 0:
                    progress_pct = min(int((current_seconds / total_seconds) * 100), 99)
                    
                    # 🔥 This will now break out of the buffer lock and print live!
                    print(f"📊 Live Transcoder Reporting Engine: [{video_id}] -> {progress_pct}%")
                    
                    # Broadcast the update down to Kafka
                    await producer.send("video-progress", {
                        "video_id": video_id,
                        "event": "VIDEO_PROGRESS_UPDATE",
                        "progress": progress_pct
                    })
            except Exception:
                pass # Guard against partial terminal string segments

    await process.wait()
    if process.returncode != 0:
        raise Exception(f"FFmpeg pipeline crashed with exit code: {process.returncode}")
        
    print(f"✅ Transcoding complete for Video ID: {video_id}")
    return manifest_path

async def process_video_event(msg_value, producer):
    video_id = os.path.splitext(msg_value.get("video_id"))[0]
    filename = msg_value.get("filename")
    if not video_id or not filename: return

    local_raw_path = f"temp_raw_{filename}"
    local_output_dir = f"temp_out_{video_id}"
    os.makedirs(local_output_dir, exist_ok=True)
    
    try:
        await asyncio.to_thread(s3_client.download_file, RAW_BUCKET, f"raw_{filename}", local_raw_path)
        
        # Run transcoding with active background tracking loops
        await transcode_with_progress(video_id, local_raw_path, local_output_dir, producer)
        
        # Fire final cleanup sync steps to flag 100% processing marks
        await producer.send(PROGRESS_TOPIC, {"video_id": video_id, "event": "VIDEO_PROGRESS_UPDATE", "progress": 100})
        
        # Offload file uploads concurrently to keep Kafka heartbeats alive
        upload_tasks = []
        for file in os.listdir(local_output_dir):
            file_path = os.path.join(local_output_dir, file)
            s3_key = f"{video_id}/{file}"
            c_type = "application/x-mpegURL" if file.endswith(".m3u8") else "video/MP2T"
            upload_tasks.append(asyncio.to_thread(sync_s3_upload, file_path, PROCESSED_BUCKET, s3_key, c_type))
        await asyncio.gather(*upload_tasks)
            
        playback_url = f"{MINIO_ENDPOINT}/{PROCESSED_BUCKET}/{video_id}/playlist.m3u8"
        await asyncio.to_thread(sync_update_db_success, video_id, playback_url)
        await producer.send_and_wait(PRODUCE_TOPIC, {"video_id": video_id, "status": "completed", "playback_url": playback_url})
        
    except Exception as e:
        print(f"❌ Core Pipeline Collapse for context key {video_id}: {str(e)}")
    finally:
        if os.path.exists(local_raw_path): os.remove(local_raw_path)
        if os.path.exists(local_output_dir): shutil.rmtree(local_output_dir)

async def main():
    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="transcoder-group", auto_offset_reset="earliest", enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await consumer.start()
    await producer.start()
    try:
        async for msg in consumer:
            await process_video_event(msg.value, producer)
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())