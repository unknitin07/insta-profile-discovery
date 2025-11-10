"""
Database Models
FIXED VERSION - Added basic password encryption
"""

from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean, Float, JSON, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import enum
import base64

Base = declarative_base()

# Simple encryption helper (for basic protection)
class SimpleEncryption:
    """
    Basic encryption for passwords
    For production, use proper encryption like cryptography.fernet
    """
    @staticmethod
    def encode(text: str) -> str:
        """Encode password (basic obfuscation)"""
        if not text:
            return text
        return base64.b64encode(text.encode()).decode()
    
    @staticmethod
    def decode(encoded: str) -> str:
        """Decode password"""
        if not encoded:
            return encoded
        try:
            return base64.b64decode(encoded.encode()).decode()
        except:
            return encoded  # Return as-is if decoding fails

# Enums for status fields
class SeedStatus(enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    CHECKED = "checked"

class AccountStatus(enum.Enum):
    PENDING = "pending"
    PASS = "pass"
    FAIL = "fail"
    CHECKED = "checked"
    PROCESSING = "processing"  # Added for better tracking

class InstagramAccountStatus(enum.Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    BANNED = "banned"
    BACKUP = "backup"

class QueueStatus(enum.Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


# 1. Seed Usernames Table
class SeedUsername(Base):
    __tablename__ = 'seed_usernames'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(255), nullable=False, unique=True, index=True)
    instagram_url = Column(Text, nullable=True)
    status = Column(Enum(SeedStatus), default=SeedStatus.PENDING, nullable=False, index=True)
    level = Column(Integer, default=0, nullable=False)  # Always 0 for seeds
    added_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    checked_at = Column(DateTime, nullable=True)
    
    def __repr__(self):
        return f"<SeedUsername(username='{self.username}', status='{self.status}')>"


# 2. Discovered Accounts Table
class DiscoveredAccount(Base):
    __tablename__ = 'discovered_accounts'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(255), nullable=False, unique=True, index=True)
    status = Column(Enum(AccountStatus), default=AccountStatus.PENDING, nullable=False, index=True)
    level = Column(Integer, nullable=False, index=True)  # 1-4 (reduced from 6)
    parent_username = Column(String(255), nullable=True, index=True)  # Who we got this from
    followers_count = Column(Integer, nullable=True)
    following_count = Column(Integer, nullable=True)
    posts_count = Column(Integer, nullable=True)
    bio = Column(Text, nullable=True)
    discovered_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    checked_at = Column(DateTime, nullable=True)
    criteria_data = Column(JSON, nullable=True)  # Store why it passed/failed
    
    def __repr__(self):
        return f"<DiscoveredAccount(username='{self.username}', status='{self.status}', level={self.level})>"


# 3. Passed Influencers Table
class PassedInfluencer(Base):
    __tablename__ = 'passed_influencers'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(255), nullable=False, unique=True, index=True)
    full_name = Column(String(255), nullable=True)
    
    # Contact details (all optional)
    telegram_link = Column(Text, nullable=True)
    email = Column(String(255), nullable=True)
    phone = Column(String(50), nullable=True)
    website = Column(Text, nullable=True)
    external_links = Column(JSON, nullable=True)  # Array of all links from bio
    
    # Profile metrics
    followers_count = Column(Integer, nullable=False)
    following_count = Column(Integer, nullable=True)
    posts_count = Column(Integer, nullable=True)
    avg_reel_views = Column(Integer, nullable=True)  # Average of last 5 reels
    engagement_rate = Column(Float, nullable=True)  # Percentage
    
    # Additional info
    bio = Column(Text, nullable=True)
    category = Column(String(100), nullable=True)  # e.g., 'fashion', 'tech'
    profile_pic_url = Column(Text, nullable=True)
    is_verified = Column(Boolean, default=False)
    is_business_account = Column(Boolean, default=False)
    
    # Meta
    level_found = Column(Integer, nullable=True)  # Which level was this found at
    contact_extracted_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    notes = Column(Text, nullable=True)  # Manual notes via bot
    
    def __repr__(self):
        return f"<PassedInfluencer(username='{self.username}', followers={self.followers_count})>"


# 4. Instagram Accounts Table (for scraping)
class InstagramAccount(Base):
    __tablename__ = 'instagram_accounts'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(255), nullable=False, unique=True)
    password = Column(String(500), nullable=False)  # FIXED: Stores encoded password
    status = Column(Enum(InstagramAccountStatus), default=InstagramAccountStatus.ACTIVE, nullable=False, index=True)
    requests_made = Column(Integer, default=0, nullable=False)  # Track usage
    last_used_at = Column(DateTime, nullable=True)
    added_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    def set_password(self, plain_password: str):
        """Set password with encoding"""
        self.password = SimpleEncryption.encode(plain_password)
    
    def get_password(self) -> str:
        """Get decoded password"""
        return SimpleEncryption.decode(self.password)
    
    def __repr__(self):
        return f"<InstagramAccount(username='{self.username}', status='{self.status}', requests={self.requests_made})>"


# 5. Processing Queue Table
class ProcessingQueue(Base):
    __tablename__ = 'processing_queue'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(255), nullable=False, index=True)
    level = Column(Integer, nullable=False, index=True)
    status = Column(Enum(QueueStatus), default=QueueStatus.QUEUED, nullable=False, index=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    
    def __repr__(self):
        return f"<ProcessingQueue(username='{self.username}', level={self.level}, status='{self.status}')>"


# 6. Script Config Table
class ScriptConfig(Base):
    __tablename__ = 'script_config'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(100), nullable=False, unique=True, index=True)
    value = Column(Text, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return f"<ScriptConfig(key='{self.key}', value='{self.value}')>"


# 7. Activity Logs Table
class ActivityLog(Base):
    __tablename__ = 'activity_logs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    action = Column(String(100), nullable=False, index=True)
    username = Column(String(255), nullable=True, index=True)
    details = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    
    def __repr__(self):
        return f"<ActivityLog(action='{self.action}', username='{self.username}')>"


# Database initialization
class Database:
    def __init__(self, db_path='data.db'):
        self.engine = create_engine(f'sqlite:///{db_path}', echo=False)
        self.Session = sessionmaker(bind=self.engine)
    
    def create_tables(self):
        """Create all tables"""
        Base.metadata.create_all(self.engine)
        print("✅ Database tables created successfully!")
    
    def get_session(self):
        """Get a new database session"""
        return self.Session()
    
    def initialize_default_config(self):
        """Initialize default configuration values"""
        session = self.get_session()
        
        try:
            default_configs = [
                ('concurrent_limit', '5'),
                ('min_followers', '500000'),
                ('min_avg_reel_views', '100000'),
                ('min_engagement_rate', '2.0'),
                ('min_seed_threshold', '10'),
                ('max_level', '4'),  # FIXED: Reduced from 6 to 4
                ('script_status', 'active'),  # active/paused
            ]
            
            for key, value in default_configs:
                existing = session.query(ScriptConfig).filter_by(key=key).first()
                if not existing:
                    config = ScriptConfig(key=key, value=value)
                    session.add(config)
            
            session.commit()
            print("✅ Default configuration initialized!")
        finally:
            session.close()


# Usage example
if __name__ == "__main__":
    # Initialize database
    db = Database('data.db')
    
    # Create all tables
    db.create_tables()
    
    # Initialize default config
    db.initialize_default_config()
    
    print("\n🎉 Database setup complete!")
    print("📁 Database file: data.db")
    print("\n📊 Tables created:")
    print("  1. seed_usernames")
    print("  2. discovered_accounts")
    print("  3. passed_influencers")
    print("  4. instagram_accounts")
    print("  5. processing_queue")
    print("  6. script_config")
    print("  7. activity_logs")
