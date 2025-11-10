"""
Instagram Scraper - Fetches profile data, reels, and followings
FIXED VERSION - Session persistence, better rate limiting, error handling
"""

import time
import os
import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import logging
from instagrapi import Client
from instagrapi.exceptions import (
    LoginRequired, 
    ChallengeRequired, 
    RateLimitError,
    ClientError,
    TwoFactorRequired
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InstagramScraper:
    """
    Handles Instagram data fetching with account rotation and session persistence
    """
    
    def __init__(self, accounts: List[Dict[str, str]]):
        """
        Initialize with Instagram accounts for rotation
        
        Args:
            accounts: List of dicts with 'username' and 'password'
        """
        self.accounts = accounts
        self.current_account_index = 0
        self.clients = {}  # username -> Client instance
        
        # FIXED: Track requests per hour instead of total
        self.account_request_times = {}  # username -> list of timestamps
        self.max_requests_per_hour = 50  # More realistic limit
        
        # Create sessions directory
        self.sessions_dir = 'sessions'
        os.makedirs(self.sessions_dir, exist_ok=True)
        
        # Initialize first account
        if accounts:
            self._login_current_account()
    
    def _get_session_file(self, username: str) -> str:
        """Get session file path for username"""
        return os.path.join(self.sessions_dir, f"{username}.json")
    
    def _check_hourly_rate_limit(self, username: str) -> bool:
        """Check if account has exceeded hourly rate limit"""
        if username not in self.account_request_times:
            return True
        
        now = time.time()
        hour_ago = now - 3600
        
        # Remove timestamps older than 1 hour
        recent_requests = [t for t in self.account_request_times[username] if t > hour_ago]
        self.account_request_times[username] = recent_requests
        
        if len(recent_requests) >= self.max_requests_per_hour:
            logger.warning(f"⏰ Account {username} hit hourly rate limit ({len(recent_requests)}/{self.max_requests_per_hour})")
            return False
        
        return True
    
    def _record_request(self, username: str):
        """Record a request timestamp for rate limiting"""
        if username not in self.account_request_times:
            self.account_request_times[username] = []
        
        self.account_request_times[username].append(time.time())
    
    def _get_current_client(self) -> Tuple[Client, str]:
        """Get current active client and username"""
        if not self.accounts:
            raise Exception("No Instagram accounts available")
        
        # Try to find an account that's not rate limited
        attempts = 0
        while attempts < len(self.accounts):
            current = self.accounts[self.current_account_index]
            username = current['username']
            
            if self._check_hourly_rate_limit(username):
                if username not in self.clients:
                    self._login_account(username, current['password'])
                
                if username in self.clients:
                    return self.clients[username], username
            
            # Try next account
            self._rotate_account()
            attempts += 1
        
        raise Exception("All accounts are rate limited or unavailable")
    
    def _login_account(self, username: str, password: str) -> bool:
        """
        Login to Instagram account with session persistence
        FIXED: Saves and loads sessions to avoid re-login
        """
        try:
            logger.info(f"Logging in to Instagram account: {username}")
            client = Client()
            client.delay_range = [2, 5]
            
            session_file = self._get_session_file(username)
            
            # Try to load existing session
            if os.path.exists(session_file):
                try:
                    logger.info(f"Loading saved session for {username}")
                    client.load_settings(session_file)
                    client.login(username, password)
                    
                    # Test if session is valid
                    client.get_timeline_feed()
                    logger.info(f"✅ Session restored successfully for {username}")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Saved session invalid for {username}, logging in fresh: {e}")
                    os.remove(session_file)
                    client = Client()
                    client.delay_range = [2, 5]
                    client.login(username, password)
                    client.dump_settings(session_file)
            else:
                # Fresh login
                logger.info(f"Fresh login for {username}")
                client.login(username, password)
                client.dump_settings(session_file)
                logger.info(f"✅ Session saved for {username}")
            
            self.clients[username] = client
            self.account_request_times[username] = []
            
            logger.info(f"✅ Successfully logged in: {username}")
            return True
            
        except TwoFactorRequired:
            logger.error(f"❌ 2FA required for {username} - please disable 2FA or handle manually")
            return False
        except (LoginRequired, ChallengeRequired) as e:
            logger.error(f"❌ Login failed for {username}: {str(e)}")
            logger.error(f"   This account may be flagged. Try logging in manually via Instagram app.")
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error logging in {username}: {str(e)}")
            return False
    
    def _login_current_account(self):
        """Login to current account in rotation"""
        if not self.accounts:
            return False
        
        current = self.accounts[self.current_account_index]
        return self._login_account(current['username'], current['password'])
    
    def _rotate_account(self):
        """Rotate to next Instagram account"""
        self.current_account_index = (self.current_account_index + 1) % len(self.accounts)
        logger.info(f"🔄 Rotating to account {self.current_account_index + 1}/{len(self.accounts)}")
        
        current = self.accounts[self.current_account_index]
        username = current['username']
        
        # Only login if not already logged in
        if username not in self.clients:
            self._login_current_account()
    
    def _safe_request(self, func, *args, max_retries=3, **kwargs):
        """
        Execute request with retry logic and account rotation
        FIXED: Better error handling and rate limit respect
        """
        retries = 0
        
        while retries < max_retries:
            try:
                client, username = self._get_current_client()
                result = func(client, *args, **kwargs)
                self._record_request(username)
                return result
                
            except RateLimitError as e:
                logger.warning(f"⚠️ Rate limit hit: {e}")
                self._rotate_account()
                retries += 1
                time.sleep(10 * retries)  # Exponential backoff
                
            except (LoginRequired, ChallengeRequired) as e:
                logger.warning(f"⚠️ Login issue: {e}")
                # Remove this client and try next account
                current = self.accounts[self.current_account_index]
                username = current['username']
                if username in self.clients:
                    del self.clients[username]
                self._rotate_account()
                retries += 1
                
            except ClientError as e:
                logger.error(f"❌ Client error: {str(e)}")
                retries += 1
                time.sleep(5 * retries)
                
            except Exception as e:
                logger.error(f"❌ Unexpected error: {str(e)}")
                retries += 1
                time.sleep(5 * retries)
        
        raise Exception(f"Failed after {max_retries} retries")
    
    def get_user_info(self, username: str) -> Optional[Dict]:
        """
        Get user profile information
        
        Returns:
            Dict with profile data or None if failed
        """
        try:
            def _fetch(client):
                user = client.user_info_by_username(username)
                return {
                    'username': user.username,
                    'user_id': user.pk,
                    'full_name': user.full_name,
                    'bio': user.biography,
                    'followers_count': user.follower_count,
                    'following_count': user.following_count,
                    'posts_count': user.media_count,
                    'is_verified': user.is_verified,
                    'is_business': user.is_business,
                    'external_url': user.external_url,
                    'profile_pic_url': user.profile_pic_url,
                    'category': user.category if hasattr(user, 'category') else None,
                }
            
            return self._safe_request(_fetch)
            
        except Exception as e:
            logger.error(f"Failed to get user info for {username}: {str(e)}")
            return None
    
    def get_user_reels(self, username: str, count: int = 5) -> List[Dict]:
        """
        Get user's recent reels
        
        Args:
            username: Instagram username
            count: Number of reels to fetch (max 5)
        
        Returns:
            List of reel data dicts
        """
        try:
            def _fetch(client):
                user_id = client.user_id_from_username(username)
                reels = client.user_clips(user_id, amount=count)
                
                reel_data = []
                for reel in reels[:count]:
                    reel_data.append({
                        'id': reel.pk,
                        'view_count': reel.play_count if hasattr(reel, 'play_count') else 0,
                        'like_count': reel.like_count,
                        'comment_count': reel.comment_count,
                        'created_at': reel.taken_at.isoformat() if reel.taken_at else None,
                        'url': f"https://www.instagram.com/reel/{reel.code}/"
                    })
                
                return reel_data
            
            return self._safe_request(_fetch)
            
        except Exception as e:
            logger.error(f"Failed to get reels for {username}: {str(e)}")
            return []
    
    def get_user_followings(self, username: str, max_count: int = 100) -> List[str]:
        """
        Get list of usernames that this user follows
        FIXED: Reduced from 1000 to 100 for better performance
        
        Args:
            username: Instagram username
            max_count: Maximum followings to fetch (default 100)
        
        Returns:
            List of usernames
        """
        try:
            def _fetch(client):
                user_id = client.user_id_from_username(username)
                followings = client.user_following(user_id, amount=max_count)
                following_usernames = [user.username for user in followings.values()]
                logger.info(f"📊 Fetched {len(following_usernames)} followings for @{username}")
                return following_usernames
            
            return self._safe_request(_fetch)
            
        except Exception as e:
            logger.error(f"Failed to get followings for {username}: {str(e)}")
            return []
    
    def get_complete_profile_data(self, username: str) -> Optional[Dict]:
        """
        Get complete profile data including info, reels, and followings
        
        Returns:
            Dict with all profile data or None if failed
        """
        try:
            logger.info(f"🔍 Fetching complete profile data for @{username}")
            
            user_info = self.get_user_info(username)
            if not user_info:
                return None
            
            reels = self.get_user_reels(username, count=5)
            followings = self.get_user_followings(username)
            
            complete_data = {
                'user_info': user_info,
                'reels': reels,
                'followings': followings,
                'followings_count': len(followings),
                'fetched_at': datetime.utcnow().isoformat()
            }
            
            logger.info(f"✅ Successfully fetched data for @{username}")
            return complete_data
            
        except Exception as e:
            logger.error(f"Failed to get complete profile for {username}: {str(e)}")
            return None
    
    def check_account_status(self, username: str) -> Dict:
        """
        Check if an Instagram scraping account is working
        
        Args:
            username: Instagram account username to check
        
        Returns:
            Dict with status info
        """
        status = {
            'username': username,
            'is_logged_in': False,
            'is_working': False,
            'requests_this_hour': 0,
            'error': None,
            'checked_at': datetime.utcnow().isoformat()
        }
        
        try:
            if username in self.clients:
                client = self.clients[username]
                client.account_info()
                status['is_logged_in'] = True
                status['is_working'] = True
                
                # Get request count
                if username in self.account_request_times:
                    now = time.time()
                    hour_ago = now - 3600
                    recent = [t for t in self.account_request_times[username] if t > hour_ago]
                    status['requests_this_hour'] = len(recent)
            else:
                status['error'] = "Not logged in"
        
        except Exception as e:
            status['error'] = str(e)
        
        return status
