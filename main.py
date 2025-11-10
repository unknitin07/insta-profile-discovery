"""
Main Entry Point - Instagram Influencer Finder
Starts both the Telegram bot and processing loop
FIXED VERSION - Session management and error handling improved
"""

import os
import sys
import logging
import threading
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.models import Database, InstagramAccount, ScriptConfig, InstagramAccountStatus
from core.scraper import InstagramScraper
from core.criteria import CriteriaChecker
from core.contact_extractor import ContactExtractor
from core.queue_manager import QueueManager
from bot.telegram_bot import TelegramBot

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def initialize_database():
    """Initialize database and tables"""
    logger.info("🗄️ Initializing database...")
    db = Database('data.db')
    db.create_tables()
    db.initialize_default_config()
    logger.info("✅ Database initialized!")
    return db


def load_instagram_accounts(db):
    """Load Instagram accounts from database"""
    session = db.get_session()
    try:
        accounts = session.query(InstagramAccount).filter(
            InstagramAccount.status == InstagramAccountStatus.ACTIVE
        ).all()
        
        account_list = [
            {'username': acc.username, 'password': acc.password}
            for acc in accounts
        ]
        
        logger.info(f"📱 Loaded {len(account_list)} Instagram accounts")
        return account_list
    finally:
        session.close()


def start_processing_loop(queue_manager):
    """Start processing loop in separate thread"""
    logger.info("🚀 Starting processing loop...")
    try:
        queue_manager.run_processing_loop()
    except KeyboardInterrupt:
        logger.info("⏹️ Processing loop interrupted")
    except Exception as e:
        logger.error(f"❌ Processing loop error: {e}", exc_info=True)


def main():
    """Main entry point"""
    logger.info("=" * 60)
    logger.info("🎯 Instagram Influencer Finder")
    logger.info("=" * 60)
    
    try:
        # 1. Initialize database
        db = initialize_database()
        
        # 2. Load Instagram accounts
        instagram_accounts = load_instagram_accounts(db)
        
        if not instagram_accounts:
            logger.warning("⚠️ No Instagram accounts found!")
            logger.warning("Add accounts via Telegram bot: /add_account <username> <password>")
            logger.warning("Starting bot-only mode...")
        
        # 3. Initialize components
        logger.info("🔧 Initializing components...")
        
        # Scraper (only if accounts exist)
        scraper = InstagramScraper(instagram_accounts) if instagram_accounts else None
        
        # Criteria Checker
        session = db.get_session()
        try:
            config_entries = session.query(ScriptConfig).all()
            config = {entry.key: entry.value for entry in config_entries}
        finally:
            session.close()
        
        criteria_config = {
            'min_followers': int(config.get('min_followers', 500000)),
            'min_avg_reel_views': int(config.get('min_avg_reel_views', 100000)),
            'min_engagement_rate': float(config.get('min_engagement_rate', 2.0))
        }
        
        criteria_checker = CriteriaChecker(criteria_config)
        contact_extractor = ContactExtractor()
        
        # Queue Manager (FIXED: Pass Database object, not session)
        if scraper:
            queue_manager = QueueManager(
                db,  # Pass Database object, not session
                scraper, 
                criteria_checker, 
                contact_extractor
            )
        else:
            # Mock queue manager if no accounts
            class MockQueueManager:
                concurrent_limit = 5
                def get_stats(self):
                    return {
                        'seed_usernames': {'pending': 0, 'processing': 0, 'checked': 0},
                        'discovered_accounts': {'pending': 0, 'passed': 0, 'failed': 0, 'checked': 0},
                        'passed_influencers': 0,
                        'config': config
                    }
                def stop(self):
                    pass
            queue_manager = MockQueueManager()
        
        # 4. Get Telegram Bot Token
        telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        
        if not telegram_token:
            logger.error("❌ TELEGRAM_BOT_TOKEN not found in .env file!")
            logger.error("Create a .env file with: TELEGRAM_BOT_TOKEN=your_token_here")
            sys.exit(1)
        
        # 5. Initialize Telegram Bot
        logger.info("🤖 Initializing Telegram bot...")
        bot = TelegramBot(telegram_token, db, queue_manager)
        
        # 6. Start processing loop in background (if accounts available)
        if scraper and instagram_accounts:
            logger.info("🔄 Starting background processing...")
            processing_thread = threading.Thread(
                target=start_processing_loop, 
                args=(queue_manager,),
                daemon=True
            )
            processing_thread.start()
        else:
            logger.warning("⏸️ Processing paused - add Instagram accounts first")
        
        # 7. Start Telegram bot (blocking)
        logger.info("✅ All systems ready!")
        logger.info("🤖 Telegram bot is running...")
        logger.info("Control via Telegram: /start, /help, /stats")
        bot.run()
        
    except KeyboardInterrupt:
        logger.info("\n⏹️ Shutting down...")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
