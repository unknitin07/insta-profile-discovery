"""
Instagram Scraper - Fetches profile data, reels, and followings
Uses instagrapi library with account rotation
"""

import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import logging
from instagrapi import Client
from instagrapi.exceptions import (
    LoginRequired, 
    ChallengeRequired, 
    RateLimitError,
    ClientError
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InstagramScraper:
    """
    Handles Instagram data fetching with account rotation
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
        self.account_requests = {}  # Track requests per account
        self.max_requests_per_account = 200  # Before rotation
        
        # Initialize first account
        if accounts:
            self._login_current_account()
    
    def _get_current_client(self) -> Tuple[Client, str]:
        """Get current active client and username"""
        if not self.accounts:
            raise Exception("No Instagram accounts available")
        
        current = self.accounts[self.current_account_index]
        username = current['username']
        
        if username not in self.clients:
            self._login_account(username, current['password'])
        
        return self.clients[username], username
    
    def _login_account(self, username: str, password: str) -> bool:
        """Login to Instagram account"""
        try:
            logger.info(f"Logging in to Instagram account: {username}")
            client = Client()
            client.delay_range = [2, 5]  # Random delay between requests
            
            # Try to login
            client.login(username, password)
            
            self.clients[username] = client
            self.account_requests[username] = 0
            
            logger.info(f"✅ Successfully logged in: {username}")
            return True
            
        except (LoginRequired, ChallengeRequired) as e:
            logger.error(f"❌ Login failed for {username}: {str(e)}")
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
        self._login_current_account()
    
    def _increment_requests(self, username: str):
        """Track requests and rotate if needed"""
        self.account_requests[username] = self.account_requests.get(username, 0) + 1
        
        if self.account_requests[username] >= self.max_requests_per_account:
            logger.warning(f"⚠️ Account {username} hit request limit, rotating...")
            self._rotate_account()
    
    def _safe_request(self, func, *args, max_retries=3, **kwargs):
        """Execute request with retry logic and account rotation"""
        retries = 0
        
        while retries < max_retries:
            try:
                client, username = self._get_current_client()
                result = func(client, *args, **kwargs)
                self._increment_requests(username)
                return result
                
            except RateLimitError:
                logger.warning("⚠️ Rate limit hit, rotating account...")
                self._rotate_account()
                retries += 1
                time.sleep(5)
                
            except (LoginRequired, ChallengeRequired):
                logger.warning("⚠️ Login required, rotating account...")
                self._rotate_account()
                retries += 1
                
            except ClientError as e:
                logger.error(f"❌ Client error: {str(e)}")
                retries += 1
                time.sleep(3)
                
            except Exception as e:
                logger.error(f"❌ Unexpected error: {str(e)}")
                retries += 1
                time.sleep(3)
        
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
            count: Number of reels to fetch
        
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
    
    def get_user_followings(self, username: str, max_count: int = 1000) -> List[str]:
        """
        Get list of usernames that this user follows
        
        Args:
            username: Instagram username
            max_count: Maximum followings to fetch
        
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
            'error': None,
            'checked_at': datetime.utcnow().isoformat()
        }
        
        try:
            if username in self.clients:
                client = self.clients[username]
                client.account_info()
                status['is_logged_in'] = True
                status['is_working'] = True
            else:
                status['error'] = "Not logged in"
        
        except Exception as e:
            status['error'] = str(e)
        
        return status