"""
Queue Manager - Handles concurrent processing and level management
FIXED VERSION - Proper session management, duplicate detection, error recovery
"""

import asyncio
import time
from typing import List, Dict
from datetime import datetime
import logging

from database.models import (
    Database, SeedUsername, DiscoveredAccount, PassedInfluencer,
    ActivityLog, ScriptConfig,
    AccountStatus, SeedStatus
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QueueManager:
    """
    Manages the processing queue and coordinates scraping/checking
    FIXED: Uses Database object instead of session for proper session management
    """
    
    def __init__(self, database: Database, scraper, criteria_checker, contact_extractor):
        """
        Initialize queue manager
        FIXED: Takes Database object, not Session
        """
        self.database = database  # Store Database object
        self.scraper = scraper
        self.criteria_checker = criteria_checker
        self.contact_extractor = contact_extractor
        
        self.config = self._load_config()
        self.concurrent_limit = int(self.config.get('concurrent_limit', 5))
        self.max_level = int(self.config.get('max_level', 4))  # FIXED: Reduced from 6 to 4
        self.is_running = False
    
    def _load_config(self) -> Dict:
        """Load configuration from database"""
        session = self.database.get_session()
        try:
            config_entries = session.query(ScriptConfig).all()
            return {entry.key: entry.value for entry in config_entries}
        finally:
            session.close()
    
    def _log_activity(self, action: str, username: str = None, details: Dict = None):
        """Log activity to database with proper session management"""
        session = self.database.get_session()
        try:
            log = ActivityLog(action=action, username=username, details=details)
            session.add(log)
            session.commit()
        except Exception as e:
            logger.error(f"Failed to log activity: {e}")
            session.rollback()
        finally:
            session.close()
    
    def get_pending_usernames(self, limit: int = None) -> List[Dict]:
        """Get pending usernames from both seed and discovered accounts"""
        if limit is None:
            limit = self.concurrent_limit
        
        session = self.database.get_session()
        try:
            pending = []
            
            # Get pending seeds first
            seeds = session.query(SeedUsername).filter(
                SeedUsername.status == SeedStatus.PENDING
            ).limit(limit).all()
            
            pending.extend([{'username': s.username, 'level': 0, 'type': 'seed'} for s in seeds])
            
            # Get pending discovered accounts if we need more
            if len(pending) < limit:
                remaining = limit - len(pending)
                discovered = session.query(DiscoveredAccount).filter(
                    DiscoveredAccount.status == AccountStatus.PENDING,
                    DiscoveredAccount.level <= self.max_level
                ).order_by(DiscoveredAccount.level).limit(remaining).all()
                
                pending.extend([{
                    'username': d.username, 
                    'level': d.level, 
                    'type': 'discovered'
                } for d in discovered])
            
            return pending
        finally:
            session.close()
    
    def process_username(self, username: str, level: int, username_type: str) -> Dict:
        """
        Process a single username: scrape, check criteria, extract contacts
        FIXED: Proper session management throughout
        """
        result = {
            'username': username,
            'level': level,
            'success': False,
            'passed': False,
            'followings_added': 0,
            'error': None
        }
        
        session = self.database.get_session()
        
        try:
            logger.info(f"🔍 Processing @{username} (Level {level})")
            
            # Mark as processing
            if username_type == 'seed':
                session.query(SeedUsername).filter(
                    SeedUsername.username == username
                ).update({'status': SeedStatus.PROCESSING})
            else:
                session.query(DiscoveredAccount).filter(
                    DiscoveredAccount.username == username
                ).update({'status': AccountStatus.PROCESSING})
            session.commit()
            
            # Scrape profile data
            profile_data = self.scraper.get_complete_profile_data(username)
            
            if not profile_data:
                result['error'] = "Failed to fetch profile data"
                self._mark_as_failed(username, username_type, result['error'])
                return result
            
            user_info = profile_data['user_info']
            reels = profile_data['reels']
            followings = profile_data['followings']
            
            # Check criteria
            passed, criteria_data = self.criteria_checker.check_account(
                username=username,
                followers_count=user_info['followers_count'],
                following_count=user_info['following_count'],
                posts_count=user_info['posts_count'],
                bio=user_info['bio'],
                last_5_reels=reels,
                profile_data=user_info
            )
            
            result['passed'] = passed
            
            # Store result
            if passed:
                contacts = self.contact_extractor.extract_all(
                    bio=user_info['bio'],
                    external_url=user_info.get('external_url'),
                    profile_data=user_info
                )
                
                self._store_passed_influencer(user_info, criteria_data, contacts, level)
                logger.info(f"✅ @{username} PASSED criteria")
                self._log_activity('account_passed', username, criteria_data['summary'])
            else:
                logger.info(f"❌ @{username} FAILED criteria: {', '.join(criteria_data['fail_reasons'][:2])}")
                self._log_activity('account_failed', username, {
                    'reasons': criteria_data['fail_reasons']
                })
            
            # Update status in discovered_accounts (if not seed)
            if username_type != 'seed':
                session.query(DiscoveredAccount).filter(
                    DiscoveredAccount.username == username
                ).update({
                    'status': AccountStatus.PASS if passed else AccountStatus.FAIL,
                    'followers_count': user_info['followers_count'],
                    'following_count': user_info['following_count'],
                    'posts_count': user_info['posts_count'],
                    'bio': user_info['bio'],
                    'checked_at': datetime.utcnow(),
                    'criteria_data': criteria_data
                })
                session.commit()
            
            # Add followings to queue (if within level limit)
            if level < self.max_level:
                added = self._add_followings_to_queue(username, followings, level + 1)
                result['followings_added'] = added
                logger.info(f"📊 Added {added} followings from @{username} to level {level + 1}")
            
            # Mark as checked
            if username_type == 'seed':
                session.query(SeedUsername).filter(
                    SeedUsername.username == username
                ).update({
                    'status': SeedStatus.CHECKED,
                    'checked_at': datetime.utcnow()
                })
            else:
                session.query(DiscoveredAccount).filter(
                    DiscoveredAccount.username == username
                ).update({'status': AccountStatus.CHECKED})
            session.commit()
            
            result['success'] = True
            return result
            
        except Exception as e:
            logger.error(f"❌ Error processing @{username}: {str(e)}")
            result['error'] = str(e)
            self._mark_as_failed(username, username_type, str(e))
            session.rollback()
            return result
        finally:
            session.close()
    
    def _mark_as_failed(self, username: str, username_type: str, error: str):
        """Mark username as failed with proper session management"""
        session = self.database.get_session()
        try:
            if username_type == 'seed':
                session.query(SeedUsername).filter(
                    SeedUsername.username == username
                ).update({'status': SeedStatus.CHECKED})
            else:
                session.query(DiscoveredAccount).filter(
                    DiscoveredAccount.username == username
                ).update({
                    'status': AccountStatus.FAIL,
                    'checked_at': datetime.utcnow(),
                    'criteria_data': {'error': error}
                })
            session.commit()
        except Exception as e:
            logger.error(f"Failed to mark as failed: {e}")
            session.rollback()
        finally:
            session.close()
    
    def _store_passed_influencer(self, user_info: Dict, criteria_data: Dict, 
                                contacts: Dict, level: int):
        """Store passed influencer in database with proper session management"""
        session = self.database.get_session()
        try:
            # Check if already exists
            existing = session.query(PassedInfluencer).filter(
                PassedInfluencer.username == user_info['username']
            ).first()
            
            if existing:
                logger.info(f"Influencer @{user_info['username']} already exists, skipping")
                return
            
            influencer = PassedInfluencer(
                username=user_info['username'],
                full_name=user_info.get('full_name'),
                telegram_link=contacts.get('telegram'),
                email=contacts.get('email'),
                phone=contacts.get('phone'),
                website=contacts.get('website'),
                external_links=contacts.get('all_links'),
                followers_count=user_info['followers_count'],
                following_count=user_info['following_count'],
                posts_count=user_info['posts_count'],
                avg_reel_views=criteria_data['summary'].get('avg_reel_views'),
                engagement_rate=criteria_data['summary'].get('engagement_rate'),
                bio=user_info.get('bio'),
                profile_pic_url=user_info.get('profile_pic_url'),
                is_verified=user_info.get('is_verified', False),
                is_business_account=user_info.get('is_business', False),
                level_found=level
            )
            
            session.add(influencer)
            session.commit()
            logger.info(f"💾 Stored passed influencer: @{user_info['username']}")
            
        except Exception as e:
            logger.error(f"Failed to store passed influencer: {e}")
            session.rollback()
        finally:
            session.close()
    
    def _add_followings_to_queue(self, parent_username: str, followings: List[str], 
                                 level: int) -> int:
        """
        Add followings to discovered_accounts for future processing
        FIXED: Added duplicate detection across all tables
        """
        session = self.database.get_session()
        added = 0
        
        try:
            for following_username in followings:
                try:
                    # FIXED: Check if already exists in discovered_accounts
                    existing_discovered = session.query(DiscoveredAccount).filter(
                        DiscoveredAccount.username == following_username
                    ).first()
                    
                    if existing_discovered:
                        continue
                    
                    # FIXED: Check if already exists in passed_influencers
                    existing_passed = session.query(PassedInfluencer).filter(
                        PassedInfluencer.username == following_username
                    ).first()
                    
                    if existing_passed:
                        continue
                    
                    # FIXED: Check if it's a seed username
                    existing_seed = session.query(SeedUsername).filter(
                        SeedUsername.username == following_username
                    ).first()
                    
                    if existing_seed:
                        continue
                    
                    # Add to queue
                    discovered = DiscoveredAccount(
                        username=following_username,
                        status=AccountStatus.PENDING,
                        level=level,
                        parent_username=parent_username
                    )
                    
                    session.add(discovered)
                    added += 1
                    
                except Exception as e:
                    logger.error(f"Failed to add following {following_username}: {e}")
                    continue
            
            session.commit()
            
        except Exception as e:
            logger.error(f"Failed to commit followings: {e}")
            session.rollback()
        finally:
            session.close()
        
        return added
    
    async def process_batch_async(self, usernames_data: List[Dict]):
        """
        Process multiple usernames concurrently
        FIXED: Proper semaphore to enforce concurrent limit
        """
        semaphore = asyncio.Semaphore(self.concurrent_limit)
        
        async def process_with_limit(data):
            async with semaphore:
                return await asyncio.to_thread(
                    self.process_username,
                    data['username'],
                    data['level'],
                    data['type']
                )
        
        tasks = [process_with_limit(data) for data in usernames_data]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results
    
    def run_processing_loop(self):
        """
        Main processing loop
        FIXED: Added error recovery and exponential backoff
        """
        self.is_running = True
        consecutive_errors = 0
        
        logger.info("🚀 Starting processing loop...")
        self._log_activity('processing_started')
        
        while self.is_running:
            try:
                pending = self.get_pending_usernames(self.concurrent_limit)
                
                if not pending:
                    logger.info("📭 No pending usernames, waiting...")
                    self._log_activity('no_pending_usernames')
                    time.sleep(30)  # Wait 30 seconds before checking again
                    continue
                
                logger.info(f"📋 Processing batch of {len(pending)} usernames")
                
                results = asyncio.run(self.process_batch_async(pending))
                
                # Count successes
                passed_count = sum(1 for r in results if isinstance(r, dict) and r.get('passed'))
                failed_count = sum(1 for r in results if isinstance(r, dict) and not r.get('passed'))
                error_count = sum(1 for r in results if isinstance(r, Exception))
                
                logger.info(f"✅ Batch complete: {passed_count} passed, {failed_count} failed, {error_count} errors")
                
                # Reset error counter on successful batch
                consecutive_errors = 0
                
                # Delay between batches to avoid rate limits
                time.sleep(10)
                
            except KeyboardInterrupt:
                logger.info("⏹️ Processing interrupted by user")
                break
                
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"❌ Error in processing loop (#{consecutive_errors}): {e}")
                self._log_activity('processing_error', details={'error': str(e), 'consecutive': consecutive_errors})
                
                # FIXED: Stop if too many consecutive errors
                if consecutive_errors >= 5:
                    logger.critical("💥 Too many consecutive errors, stopping processing loop!")
                    self._log_activity('critical_error', details={'error': 'Too many consecutive errors'})
                    break
                
                # FIXED: Exponential backoff
                backoff_time = 30 * consecutive_errors
                logger.warning(f"⏳ Backing off for {backoff_time} seconds...")
                time.sleep(backoff_time)
        
        logger.info("🛑 Processing loop stopped")
        self._log_activity('processing_stopped')
    
    def stop(self):
        """Stop the processing loop"""
        self.is_running = False
        logger.info("🛑 Stopping processing loop...")
    
    def get_stats(self) -> Dict:
        """Get current processing statistics with proper session management"""
        session = self.database.get_session()
        try:
            stats = {
                'seed_usernames': {
                    'pending': session.query(SeedUsername).filter(SeedUsername.status == SeedStatus.PENDING).count(),
                    'processing': session.query(SeedUsername).filter(SeedUsername.status == SeedStatus.PROCESSING).count(),
                    'checked': session.query(SeedUsername).filter(SeedUsername.status == SeedStatus.CHECKED).count(),
                },
                'discovered_accounts': {
                    'pending': session.query(DiscoveredAccount).filter(DiscoveredAccount.status == AccountStatus.PENDING).count(),
                    'passed': session.query(DiscoveredAccount).filter(DiscoveredAccount.status == AccountStatus.PASS).count(),
                    'failed': session.query(DiscoveredAccount).filter(DiscoveredAccount.status == AccountStatus.FAIL).count(),
                    'checked': session.query(DiscoveredAccount).filter(DiscoveredAccount.status == AccountStatus.CHECKED).count(),
                },
                'passed_influencers': session.query(PassedInfluencer).count(),
                'config': self.config
            }
            
            return stats
        finally:
            session.close()
