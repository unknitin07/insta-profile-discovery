"""
Telegram Bot - Admin panel for controlling the script
WITH ADMIN-ONLY ACCESS CONTROL
"""

import logging
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler, 
    filters, ContextTypes, ConversationHandler
)
from datetime import datetime
import csv
import io
import os

from database.models import (
    Database, SeedUsername, InstagramAccount, 
    PassedInfluencer, ScriptConfig, SeedStatus, InstagramAccountStatus
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Conversation states
ADD_SEED, ADD_BULK_SEEDS, ADD_ACCOUNT = range(3)


class TelegramBot:
    """
    Telegram bot for controlling the Instagram scraper
    WITH ADMIN-ONLY ACCESS
    """
    
    def __init__(self, token: str, db: Database, queue_manager):
        """Initialize Telegram bot"""
        self.token = token
        self.db = db
        self.queue_manager = queue_manager
        self.app = Application.builder().token(token).build()
        
        # ADMIN CONTROL - Get admin user ID from environment variable
        self.admin_user_id = os.getenv('ADMIN_USER_ID')
        
        if not self.admin_user_id:
            logger.warning("⚠️ ADMIN_USER_ID not set! Bot will be open to everyone!")
        else:
            self.admin_user_id = int(self.admin_user_id)
            logger.info(f"✅ Admin user ID set: {self.admin_user_id}")
        
        self._register_handlers()
    
    def _is_admin(self, user_id: int) -> bool:
        """Check if user is admin"""
        if not self.admin_user_id:
            # If no admin set, allow everyone (for first setup)
            return True
        return user_id == self.admin_user_id
    
    async def _check_admin(self, update: Update) -> bool:
        """Check if user is admin and send error if not"""
        user_id = update.effective_user.id
        
        if not self._is_admin(user_id):
            await update.message.reply_text(
                "🚫 Access Denied!\n\n"
                "This bot is for admin use only.\n"
                f"Your User ID: {user_id}\n\n"
                "Contact the bot owner to get access."
            )
            logger.warning(f"⚠️ Unauthorized access attempt by user {user_id} (@{update.effective_user.username})")
            return False
        return True
    
    def _register_handlers(self):
        """Register all command handlers"""
        
        # Basic commands
        self.app.add_handler(CommandHandler("start", self.start_command))
        self.app.add_handler(CommandHandler("help", self.help_command))
        self.app.add_handler(CommandHandler("stats", self.stats_command))
        self.app.add_handler(CommandHandler("myid", self.myid_command))
        
        # Seed management
        self.app.add_handler(CommandHandler("add_seed", self.add_seed_command))
        self.app.add_handler(CommandHandler("add_seed_bulk", self.add_seed_bulk_start))
        
        # Instagram account management
        self.app.add_handler(CommandHandler("add_account", self.add_account_command))
        self.app.add_handler(CommandHandler("check_accounts", self.check_accounts_command))
        self.app.add_handler(CommandHandler("list_accounts", self.list_accounts_command))
        
        # Config management
        self.app.add_handler(CommandHandler("set_concurrent", self.set_concurrent_command))
        self.app.add_handler(CommandHandler("set_criteria", self.set_criteria_command))
        self.app.add_handler(CommandHandler("view_config", self.view_config_command))
        
        # Processing control
        self.app.add_handler(CommandHandler("pause", self.pause_command))
        self.app.add_handler(CommandHandler("resume", self.resume_command))
        
        # Results
        self.app.add_handler(CommandHandler("passed", self.passed_command))
        self.app.add_handler(CommandHandler("export", self.export_command))
        
        # Conversation handler for bulk seeds
        conv_handler = ConversationHandler(
            entry_points=[CommandHandler("add_seed_bulk", self.add_seed_bulk_start)],
            states={
                ADD_BULK_SEEDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.add_seed_bulk_process)],
            },
            fallbacks=[CommandHandler("cancel", self.cancel_command)],
        )
        self.app.add_handler(conv_handler)
    
    async def myid_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show user's Telegram ID - No admin check for this command"""
        user = update.effective_user
        message = f"""
👤 Your Telegram Information:

User ID: {user.id}
Username: @{user.username if user.username else 'No username'}
First Name: {user.first_name}
Last Name: {user.last_name if user.last_name else 'N/A'}

ℹ️ To set yourself as admin:
Add this to your .env file:
ADMIN_USER_ID={user.id}

Then restart the bot.
        """
        await update.message.reply_text(message)
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start command - welcome message"""
        if not await self._check_admin(update):
            return
        
        user = update.effective_user
        message = f"""
🤖 Instagram Influencer Finder Bot

✅ Welcome Admin: {user.first_name}!

I help you find and track influencers based on your criteria.

📋 Available Commands:

📊 Status & Info
/stats - View processing statistics
/help - Show all commands
/myid - Get your Telegram User ID

🌱 Seed Management
/add_seed <username> - Add single seed username
/add_seed_bulk - Add multiple seeds

👤 Instagram Accounts
/add_account <user> <pass> - Add Instagram account
/check_accounts - Check account status
/list_accounts - List all accounts

⚙️ Configuration
/set_concurrent <num> - Set concurrent limit
/set_criteria - Update pass criteria
/view_config - View current config

🎮 Control
/pause - Pause processing
/resume - Resume processing

🎯 Results
/passed - View recent passed influencers
/export - Export results to CSV

Type /help for detailed usage!
        """
        await update.message.reply_text(message)
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Help command - detailed instructions"""
        if not await self._check_admin(update):
            return
        
        message = """
📖 Detailed Help

Adding Seeds:
• /add_seed cristiano - Adds single username
• /add_seed_bulk - Then paste list (one per line)

Adding Instagram Accounts:
• /add_account myuser mypass - Adds scraping account

Viewing Stats:
• /stats - Shows pending, passed, failed counts

Exporting:
• /export - Downloads CSV with passed influencers

Configuration:
• /set_concurrent 10 - Process 10 at once
• /set_criteria - Updates follower/view thresholds

Get Your ID:
• /myid - Shows your Telegram User ID (for admin setup)

Need help? Contact your administrator.
        """
        await update.message.reply_text(message)
    
    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show processing statistics"""
        if not await self._check_admin(update):
            return
        
        try:
            stats = self.queue_manager.get_stats()
            
            message = f"""
📊 Processing Statistics

Seed Usernames:
• Pending: {stats['seed_usernames']['pending']}
• Processing: {stats['seed_usernames']['processing']}
• Checked: {stats['seed_usernames']['checked']}

Discovered Accounts:
• Pending: {stats['discovered_accounts']['pending']}
• Passed: {stats['discovered_accounts']['passed']}
• Failed: {stats['discovered_accounts']['failed']}
• Checked: {stats['discovered_accounts']['checked']}

🎯 Passed Influencers: {stats['passed_influencers']}

Configuration:
• Concurrent Limit: {stats['config'].get('concurrent_limit', 'N/A')}
• Min Followers: {stats['config'].get('min_followers', 'N/A')}
• Min Reel Views: {stats['config'].get('min_avg_reel_views', 'N/A')}
• Min Engagement: {stats['config'].get('min_engagement_rate', 'N/A')}%
            """
            
            await update.message.reply_text(message)
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error getting stats: {str(e)}")
    
    async def add_seed_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Add single seed username"""
        if not await self._check_admin(update):
            return
        
        if not context.args:
            await update.message.reply_text("Usage: /add_seed <username>")
            return
        
        username = context.args[0].replace('@', '')
        
        try:
            session = self.db.get_session()
            
            existing = session.query(SeedUsername).filter(
                SeedUsername.username == username
            ).first()
            
            if existing:
                await update.message.reply_text(f"⚠️ @{username} already exists!")
                session.close()
                return
            
            seed = SeedUsername(
                username=username,
                instagram_url=f"https://instagram.com/{username}",
                status=SeedStatus.PENDING
            )
            session.add(seed)
            session.commit()
            session.close()
            
            await update.message.reply_text(f"✅ Added seed: @{username}")
            logger.info(f"Admin added seed username: {username}")
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")
    
    async def add_seed_bulk_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start bulk seed addition"""
        if not await self._check_admin(update):
            return ConversationHandler.END
        
        await update.message.reply_text(
            "📝 Please send usernames (one per line).\n"
            "Send /cancel to abort."
        )
        return ADD_BULK_SEEDS
    
    async def add_seed_bulk_process(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process bulk seed usernames"""
        if not await self._check_admin(update):
            return ConversationHandler.END
        
        text = update.message.text
        usernames = [line.strip().replace('@', '') for line in text.split('\n') if line.strip()]
        
        if not usernames:
            await update.message.reply_text("❌ No valid usernames found!")
            return ConversationHandler.END
        
        try:
            session = self.db.get_session()
            added = 0
            skipped = 0
            
            for username in usernames:
                existing = session.query(SeedUsername).filter(
                    SeedUsername.username == username
                ).first()
                
                if existing:
                    skipped += 1
                    continue
                
                seed = SeedUsername(
                    username=username,
                    instagram_url=f"https://instagram.com/{username}",
                    status=SeedStatus.PENDING
                )
                session.add(seed)
                added += 1
            
            session.commit()
            session.close()
            
            await update.message.reply_text(
                f"✅ Added: {added}\n⚠️ Skipped (duplicates): {skipped}"
            )
            logger.info(f"Admin added {added} seed usernames in bulk")
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")
        
        return ConversationHandler.END
    
    async def cancel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cancel conversation"""
        await update.message.reply_text("❌ Cancelled.")
        return ConversationHandler.END
    
    async def add_account_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Add Instagram scraping account"""
        if not await self._check_admin(update):
            return
        
        if len(context.args) < 2:
            await update.message.reply_text("Usage: /add_account <username> <password>")
            return
        
        username = context.args[0]
        password = context.args[1]
        
        try:
            session = self.db.get_session()
            
            existing = session.query(InstagramAccount).filter(
                InstagramAccount.username == username
            ).first()
            
            if existing:
                await update.message.reply_text(f"⚠️ Account {username} already exists!")
                session.close()
                return
            
            account = InstagramAccount(
                username=username,
                password=password,
                status=InstagramAccountStatus.ACTIVE
            )
            session.add(account)
            session.commit()
            session.close()
            
            await update.message.reply_text(f"✅ Added Instagram account: {username}")
            logger.info(f"Admin added Instagram account: {username}")
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")
    
    async def check_accounts_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Check Instagram account status"""
        if not await self._check_admin(update):
            return
        
        try:
            session = self.db.get_session()
            accounts = session.query(InstagramAccount).all()
            session.close()
            
            if not accounts:
                await update.message.reply_text("❌ No Instagram accounts added yet!")
                return
            
            message = "📱 Instagram Accounts Status:\n\n"
            
            for account in accounts:
                status_emoji = "✅" if account.status == InstagramAccountStatus.ACTIVE else "❌"
                message += f"{status_emoji} {account.username} - {account.status.value}\n"
                message += f"   Requests: {account.requests_made}\n"
                if account.last_used_at:
                    message += f"   Last used: {account.last_used_at.strftime('%Y-%m-%d %H:%M')}\n"
                message += "\n"
            
            await update.message.reply_text(message)
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")
    
    async def list_accounts_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """List all Instagram accounts"""
        await self.check_accounts_command(update, context)
    
    async def set_concurrent_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Set concurrent processing limit"""
        if not await self._check_admin(update):
            return
        
        if not context.args:
            await update.message.reply_text("Usage: /set_concurrent <number>")
            return
        
        try:
            limit = int(context.args[0])
            
            if limit < 1 or limit > 20:
                await update.message.reply_text("❌ Limit must be between 1 and 20")
                return
            
            session = self.db.get_session()
            config = session.query(ScriptConfig).filter(
                ScriptConfig.key == 'concurrent_limit'
            ).first()
            
            if config:
                config.value = str(limit)
                config.updated_at = datetime.utcnow()
            else:
                config = ScriptConfig(key='concurrent_limit', value=str(limit))
                session.add(config)
            
            session.commit()
            session.close()
            
            self.queue_manager.concurrent_limit = limit
            
            await update.message.reply_text(f"✅ Concurrent limit set to: {limit}")
            logger.info(f"Admin set concurrent limit to: {limit}")
            
        except ValueError:
            await update.message.reply_text("❌ Invalid number!")
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")
    
    async def set_criteria_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show current criteria settings"""
        if not await self._check_admin(update):
            return
        
        session = self.db.get_session()
        
        min_followers = session.query(ScriptConfig).filter(ScriptConfig.key == 'min_followers').first()
        min_views = session.query(ScriptConfig).filter(ScriptConfig.key == 'min_avg_reel_views').first()
        min_engagement = session.query(ScriptConfig).filter(ScriptConfig.key == 'min_engagement_rate').first()
        
        session.close()
        
        message = f"""
⚙️ Current Criteria:

• Min Followers: {min_followers.value if min_followers else 'N/A'}
• Min Avg Reel Views: {min_views.value if min_views else 'N/A'}
• Min Engagement Rate: {min_engagement.value if min_engagement else 'N/A'}%

To update, use database directly or contact admin.
        """
        
        await update.message.reply_text(message)
    
    async def view_config_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """View all configuration"""
        await self.set_criteria_command(update, context)
    
    async def pause_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Pause processing"""
        if not await self._check_admin(update):
            return
        
        self.queue_manager.stop()
        await update.message.reply_text("⏸️ Processing paused!")
        logger.info("Admin paused processing")
    
    async def resume_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Resume processing"""
        if not await self._check_admin(update):
            return
        
        await update.message.reply_text("▶️ Processing resumed! (Restart script to continue)")
        logger.info("Admin resumed processing")
    
    async def passed_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show recent passed influencers"""
        if not await self._check_admin(update):
            return
        
        try:
            session = self.db.get_session()
            influencers = session.query(PassedInfluencer).order_by(
                PassedInfluencer.contact_extracted_at.desc()
            ).limit(10).all()
            session.close()
            
            if not influencers:
                await update.message.reply_text("❌ No passed influencers yet!")
                return
            
            message = "🎯 Recent Passed Influencers:\n\n"
            
            for inf in influencers:
                message += f"👤 @{inf.username}\n"
                message += f"   Followers: {inf.followers_count:,}\n"
                message += f"   Avg Views: {inf.avg_reel_views:,}\n"
                message += f"   Engagement: {inf.engagement_rate}%\n"
                if inf.telegram_link:
                    message += f"   💬 Telegram: {inf.telegram_link}\n"
                if inf.email:
                    message += f"   📧 Email: {inf.email}\n"
                message += "\n"
            
            await update.message.reply_text(message)
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")
    
    async def export_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Export passed influencers to CSV"""
        if not await self._check_admin(update):
            return
        
        try:
            session = self.db.get_session()
            influencers = session.query(PassedInfluencer).all()
            session.close()
            
            if not influencers:
                await update.message.reply_text("❌ No data to export!")
                return
            
            # Create CSV
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Header
            writer.writerow([
                'Username', 'Full Name', 'Followers', 'Avg Reel Views', 
                'Engagement Rate', 'Telegram', 'Email', 'Phone', 
                'Website', 'Bio', 'Level Found', 'Date'
            ])
            
            # Data
            for inf in influencers:
                writer.writerow([
                    inf.username,
                    inf.full_name or '',
                    inf.followers_count,
                    inf.avg_reel_views or '',
                    inf.engagement_rate or '',
                    inf.telegram_link or '',
                    inf.email or '',
                    inf.phone or '',
                    inf.website or '',
                    inf.bio or '',
                    inf.level_found or '',
                    inf.contact_extracted_at.strftime('%Y-%m-%d')
                ])
            
            # Send as file
            output.seek(0)
            await update.message.reply_document(
                document=output.getvalue().encode('utf-8'),
                filename=f'influencers_{datetime.now().strftime("%Y%m%d")}.csv',
                caption=f"📊 Exported {len(influencers)} influencers"
            )
            
            logger.info(f"Admin exported {len(influencers)} influencers")
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error exporting: {str(e)}")
    
    def run(self):
        """Start the bot"""
        logger.info("🤖 Starting Telegram bot...")
        if self.admin_user_id:
            logger.info(f"🔒 Admin-only mode enabled. Admin ID: {self.admin_user_id}")
        else:
            logger.warning("⚠️ No admin set! Use /myid to get your ID and add to .env")
        self.app.run_polling()