# app/database.py
from sqlalchemy import create_engine, Column, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime

# Pointing directly to your local Postgres container on default port 5432
DATABASE_URL = "postgresql://postgres:supersecretpassword@localhost:5432/postgres"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class VideoMetadata(Base):
    __tablename__ = "videos"

    video_id = Column(String, primary_key=True, index=True)
    status = Column(String, default="PROCESSING")  # PROCESSING, COMPLETED, FAILED
    playback_url = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

# Utility function to drop into database tables automatically on launch
def init_db():
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()