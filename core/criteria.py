"""
Criteria Checker - Validates Instagram accounts against pass/fail criteria
"""

from datetime import datetime
from typing import Dict, List, Optional, Tuple
import json
import re


class CriteriaChecker:
    """
    Checks if an Instagram account passes the influencer criteria
    """
    
    def __init__(self, config: Dict[str, any] = None):
        """
        Initialize with configuration values
        
        Args:
            config: Dictionary with criteria thresholds
        """
        self.config = config or {
            'min_followers': 500000,
            'min_avg_reel_views': 100000,
            'min_engagement_rate': 2.0,
        }
    
    def check_account(self, 
                     username: str,
                     followers_count: int,
                     following_count: int,
                     posts_count: int,
                     bio: str,
                     last_5_reels: List[Dict],
                     profile_data: Dict = None) -> Tuple[bool, Dict]:
        """
        Check if account passes all criteria
        
        Args:
            username: Instagram username
            followers_count: Number of followers
            following_count: Number of following
            posts_count: Number of posts
            bio: Account bio text
            last_5_reels: List of last 5 reels with view/like/comment data
            profile_data: Additional profile data (optional)
        
        Returns:
            Tuple of (passed: bool, criteria_data: dict)
        """
        
        criteria_data = {
            'username': username,
            'checked_at': datetime.utcnow().isoformat(),
            'criteria_results': {},
            'pass': False,
            'fail_reasons': []
        }
        
        # 1. Check Followers Count
        followers_pass = self._check_followers(followers_count, criteria_data)
        
        # 2. Check Average Reel Views
        avg_views, views_pass = self._check_reel_views(last_5_reels, criteria_data)
        
        # 3. Check Engagement Rate
        engagement_rate, engagement_pass = self._check_engagement_rate(
            last_5_reels, 
            followers_count, 
            criteria_data
        )
        
        # 4. Extract Telegram Link (optional - doesn't affect pass/fail)
        telegram_link = self._extract_telegram_link(bio)
        criteria_data['telegram_link'] = telegram_link
        
        # 5. Extract other contact details (optional)
        contacts = self._extract_contacts(bio)
        criteria_data['contacts'] = contacts
        
        # Final decision - ALL required criteria must pass
        all_passed = followers_pass and views_pass and engagement_pass
        
        criteria_data['pass'] = all_passed
        criteria_data['summary'] = {
            'followers': followers_count,
            'avg_reel_views': avg_views,
            'engagement_rate': round(engagement_rate, 2) if engagement_rate else None,
            'has_telegram': telegram_link is not None
        }
        
        return all_passed, criteria_data
    
    def _check_followers(self, followers_count: int, criteria_data: Dict) -> bool:
        """Check if followers count meets minimum"""
        min_followers = self.config['min_followers']
        passed = followers_count >= min_followers
        
        criteria_data['criteria_results']['followers'] = {
            'required': min_followers,
            'actual': followers_count,
            'passed': passed
        }
        
        if not passed:
            criteria_data['fail_reasons'].append(
                f"Followers {followers_count:,} < required {min_followers:,}"
            )
        
        return passed
    
    def _check_reel_views(self, last_5_reels: List[Dict], criteria_data: Dict) -> Tuple[int, bool]:
        """Check average views on last 5 reels"""
        if not last_5_reels or len(last_5_reels) == 0:
            criteria_data['criteria_results']['reel_views'] = {
                'required': self.config['min_avg_reel_views'],
                'actual': 0,
                'passed': False,
                'note': 'No reels found'
            }
            criteria_data['fail_reasons'].append("No reels found to calculate views")
            return 0, False
        
        # Calculate average views
        total_views = sum(reel.get('view_count', 0) for reel in last_5_reels)
        avg_views = total_views // len(last_5_reels)
        
        min_views = self.config['min_avg_reel_views']
        passed = avg_views >= min_views
        
        criteria_data['criteria_results']['reel_views'] = {
            'required': min_views,
            'actual': avg_views,
            'reels_checked': len(last_5_reels),
            'passed': passed,
            'individual_views': [reel.get('view_count', 0) for reel in last_5_reels]
        }
        
        if not passed:
            criteria_data['fail_reasons'].append(
                f"Avg reel views {avg_views:,} < required {min_views:,}"
            )
        
        return avg_views, passed
    
    def _check_engagement_rate(self, last_5_reels: List[Dict], followers_count: int, 
                               criteria_data: Dict) -> Tuple[float, bool]:
        """Calculate and check engagement rate"""
        if not last_5_reels or len(last_5_reels) == 0 or followers_count == 0:
            criteria_data['criteria_results']['engagement_rate'] = {
                'required': self.config['min_engagement_rate'],
                'actual': 0.0,
                'passed': False,
                'note': 'Insufficient data for calculation'
            }
            criteria_data['fail_reasons'].append("Cannot calculate engagement rate")
            return 0.0, False
        
        # Calculate engagement: (likes + comments) / followers * 100
        total_engagement = 0
        for reel in last_5_reels:
            likes = reel.get('like_count', 0)
            comments = reel.get('comment_count', 0)
            total_engagement += (likes + comments)
        
        # Average engagement per reel
        avg_engagement = total_engagement / len(last_5_reels)
        
        # Engagement rate as percentage
        engagement_rate = (avg_engagement / followers_count) * 100
        
        min_rate = self.config['min_engagement_rate']
        passed = engagement_rate >= min_rate
        
        criteria_data['criteria_results']['engagement_rate'] = {
            'required': min_rate,
            'actual': round(engagement_rate, 2),
            'avg_likes_comments_per_reel': round(avg_engagement),
            'passed': passed
        }
        
        if not passed:
            criteria_data['fail_reasons'].append(
                f"Engagement rate {engagement_rate:.2f}% < required {min_rate}%"
            )
        
        return engagement_rate, passed
    
    def _extract_telegram_link(self, bio: str) -> Optional[str]:
        """
        Extract Telegram link from bio
        Looks for links containing 'https://t.'
        """
        if not bio:
            return None
        
        # Pattern to match Telegram links
        # Matches: https://t.me/username, https://t.me/+invite, etc.
        pattern = r'https://t\.[^\s]+'
        
        match = re.search(pattern, bio, re.IGNORECASE)
        if match:
            return match.group(0)
        
        return None
    
    def _extract_contacts(self, bio: str) -> Dict[str, Optional[str]]:
        """
        Extract email, phone, website from bio
        """
        contacts = {
            'email': None,
            'phone': None,
            'website': None,
            'all_links': []
        }
        
        if not bio:
            return contacts
        
        # Extract email
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        email_match = re.search(email_pattern, bio)
        if email_match:
            contacts['email'] = email_match.group(0)
        
        # Extract phone (basic pattern - can be improved)
        phone_pattern = r'[\+\(]?[1-9][0-9 .\-\(\)]{8,}[0-9]'
        phone_match = re.search(phone_pattern, bio)
        if phone_match:
            contacts['phone'] = phone_match.group(0).strip()
        
        # Extract all URLs
        url_pattern = r'https?://[^\s]+'
        urls = re.findall(url_pattern, bio)
        if urls:
            contacts['all_links'] = urls
            # First non-telegram link can be considered as website
            for url in urls:
                if 'https://t.' not in url.lower():
                    contacts['website'] = url
                    break
        
        return contacts