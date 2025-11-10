"""
Enhanced Contact Extractor - Extracts contact details from Instagram profiles
Extracts: Email, Phone, Telegram, WhatsApp, Website, and other social links
"""

import re
from typing import Dict, List, Optional
from urllib.parse import urlparse


class ContactExtractor:
    """
    Extracts contact information from Instagram bio and profile data
    """
    
    def __init__(self):
        """Initialize with regex patterns"""
        
        # Email pattern
        self.email_pattern = re.compile(
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            re.IGNORECASE
        )
        
        # Phone patterns (multiple formats)
        self.phone_patterns = [
            # International format: +1-234-567-8900, +91 98765 43210
            re.compile(r'[\+]?[(]?[0-9]{1,4}[)]?[-\s\.]?[(]?[0-9]{1,4}[)]?[-\s\.]?[0-9]{1,4}[-\s\.]?[0-9]{1,9}'),
            # WhatsApp number indicator
            re.compile(r'(?:wa\.me/|whatsapp.*?(\+?\d[\d\s-]+))', re.IGNORECASE),
        ]
        
        # URL pattern
        self.url_pattern = re.compile(
            r'https?://[^\s<>"{}|\\^`\[\]]+',
            re.IGNORECASE
        )
        
        # Telegram patterns
        self.telegram_patterns = [
            re.compile(r'https://t\.me/[^\s]+', re.IGNORECASE),
            re.compile(r'@([a-zA-Z0-9_]{5,32})', re.IGNORECASE),  # @username format
            re.compile(r'telegram.*?[@]?([a-zA-Z0-9_]{5,32})', re.IGNORECASE),
        ]
        
        # WhatsApp patterns
        self.whatsapp_patterns = [
            re.compile(r'https://wa\.me/(\d+)', re.IGNORECASE),
            re.compile(r'https://api\.whatsapp\.com/send\?phone=(\d+)', re.IGNORECASE),
            re.compile(r'whatsapp.*?(\+?\d[\d\s-]{8,})', re.IGNORECASE),
        ]
        
        # Social media domains
        self.social_domains = {
            'youtube': ['youtube.com', 'youtu.be'],
            'twitter': ['twitter.com', 'x.com'],
            'facebook': ['facebook.com', 'fb.com', 'fb.me'],
            'linkedin': ['linkedin.com'],
            'tiktok': ['tiktok.com'],
            'snapchat': ['snapchat.com'],
            'pinterest': ['pinterest.com'],
            'twitch': ['twitch.tv'],
        }
    
    def extract_all(self, bio: str, external_url: str = None, 
                   profile_data: Dict = None) -> Dict:
        """
        Extract all contact information from bio and profile
        
        Args:
            bio: Instagram bio text
            external_url: External link from Instagram profile
            profile_data: Additional profile data (optional)
        
        Returns:
            Dictionary with all extracted contacts
        """
        
        contacts = {
            'email': None,
            'emails': [],  # All found emails
            'phone': None,
            'phones': [],  # All found phones
            'telegram': None,
            'telegram_username': None,
            'whatsapp': None,
            'website': None,
            'social_links': {},
            'all_links': [],
            'extracted_at': None
        }
        
        # Combine all text sources
        all_text = self._combine_text_sources(bio, external_url, profile_data)
        
        # Extract emails
        contacts['emails'] = self._extract_emails(all_text)
        if contacts['emails']:
            contacts['email'] = contacts['emails'][0]  # Primary email
        
        # Extract phones
        contacts['phones'] = self._extract_phones(all_text)
        if contacts['phones']:
            contacts['phone'] = contacts['phones'][0]  # Primary phone
        
        # Extract all URLs
        contacts['all_links'] = self._extract_urls(all_text)
        
        # Extract Telegram
        telegram_data = self._extract_telegram(all_text, contacts['all_links'])
        contacts['telegram'] = telegram_data.get('link')
        contacts['telegram_username'] = telegram_data.get('username')
        
        # Extract WhatsApp
        contacts['whatsapp'] = self._extract_whatsapp(all_text, contacts['all_links'])
        
        # Categorize social links
        contacts['social_links'] = self._categorize_social_links(contacts['all_links'])
        
        # Extract website (non-social link)
        contacts['website'] = self._extract_website(
            contacts['all_links'], 
            contacts['social_links'],
            external_url
        )
        
        return contacts
    
    def _combine_text_sources(self, bio: str, external_url: str, 
                             profile_data: Dict) -> str:
        """Combine all text sources for extraction"""
        text_parts = []
        
        if bio:
            text_parts.append(bio)
        
        if external_url:
            text_parts.append(external_url)
        
        if profile_data:
            if 'full_name' in profile_data:
                text_parts.append(profile_data['full_name'])
            if 'category' in profile_data:
                text_parts.append(profile_data['category'])
        
        return ' '.join(text_parts)
    
    def _extract_emails(self, text: str) -> List[str]:
        """Extract all email addresses"""
        if not text:
            return []
        
        emails = self.email_pattern.findall(text)
        
        # Remove duplicates and filter invalid ones
        unique_emails = []
        seen = set()
        
        for email in emails:
            email_lower = email.lower()
            if email_lower not in seen and not self._is_invalid_email(email_lower):
                unique_emails.append(email)
                seen.add(email_lower)
        
        return unique_emails
    
    def _is_invalid_email(self, email: str) -> bool:
        """Check if email is likely a false positive"""
        invalid_patterns = [
            'noreply@', 'no-reply@', 'example.com', 'test.com',
            'yourdomain.com', 'yoursite.com'
        ]
        return any(pattern in email.lower() for pattern in invalid_patterns)
    
    def _extract_phones(self, text: str) -> List[str]:
        """Extract all phone numbers"""
        if not text:
            return []
        
        phones = []
        seen = set()
        
        for pattern in self.phone_patterns:
            matches = pattern.findall(text)
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0] if match[0] else match[1]
                
                phone = self._clean_phone(str(match))
                
                if phone and len(phone) >= 8 and phone not in seen:
                    phones.append(phone)
                    seen.add(phone)
        
        return phones
    
    def _clean_phone(self, phone: str) -> str:
        """Clean and format phone number"""
        phone = re.sub(r'(call|text|phone|mobile|tel)[:\s]*', '', phone, flags=re.IGNORECASE)
        phone = re.sub(r'[^\d\+\-\(\)\s]', '', phone)
        return phone.strip()
    
    def _extract_urls(self, text: str) -> List[str]:
        """Extract all URLs"""
        if not text:
            return []
        
        urls = self.url_pattern.findall(text)
        
        unique_urls = []
        seen = set()
        
        for url in urls:
            url = url.rstrip('.,!?;:)')
            url_lower = url.lower()
            if url_lower not in seen:
                unique_urls.append(url)
                seen.add(url_lower)
        
        return unique_urls
    
    def _extract_telegram(self, text: str, urls: List[str]) -> Dict:
        """Extract Telegram link and username"""
        telegram_data = {
            'link': None,
            'username': None
        }
        
        # Check URLs first (most reliable)
        for url in urls:
            if 'https://t.' in url.lower():
                telegram_data['link'] = url
                match = re.search(r't\.me/([a-zA-Z0-9_]+)', url, re.IGNORECASE)
                if match:
                    telegram_data['username'] = match.group(1)
                break
        
        # If no link found, try to extract username from text
        if not telegram_data['username'] and text:
            for pattern in self.telegram_patterns:
                match = pattern.search(text)
                if match:
                    username = match.group(1) if match.lastindex else match.group(0)
                    username = username.replace('@', '')
                    if 5 <= len(username) <= 32:
                        telegram_data['username'] = username
                        if not telegram_data['link']:
                            telegram_data['link'] = f"https://t.me/{username}"
                        break
        
        return telegram_data
    
    def _extract_whatsapp(self, text: str, urls: List[str]) -> Optional[str]:
        """Extract WhatsApp link or number"""
        for url in urls:
            if 'wa.me' in url.lower() or 'whatsapp' in url.lower():
                return url
        
        if text:
            for pattern in self.whatsapp_patterns:
                match = pattern.search(text)
                if match:
                    number = match.group(1) if match.lastindex else match.group(0)
                    number = re.sub(r'[^\d+]', '', number)
                    if len(number) >= 8:
                        return f"https://wa.me/{number}"
        
        return None
    
    def _categorize_social_links(self, urls: List[str]) -> Dict[str, str]:
        """Categorize URLs by social media platform"""
        social_links = {}
        
        for url in urls:
            parsed = urlparse(url.lower())
            domain = parsed.netloc.replace('www.', '')
            
            for platform, domains in self.social_domains.items():
                if any(d in domain for d in domains):
                    if platform not in social_links:
                        social_links[platform] = url
                    break
        
        return social_links
    
    def _extract_website(self, all_links: List[str], social_links: Dict, 
                        external_url: str) -> Optional[str]:
        """Extract website URL (non-social link)"""
        if external_url:
            parsed = urlparse(external_url.lower())
            domain = parsed.netloc.replace('www.', '')
            
            is_social = False
            for domains in self.social_domains.values():
                if any(d in domain for d in domains):
                    is_social = True
                    break
            
            if 't.me' in domain or 'wa.me' in domain or 'whatsapp' in domain:
                is_social = True
            
            if not is_social:
                return external_url
        
        social_urls_lower = [url.lower() for url in social_links.values()]
        
        for url in all_links:
            url_lower = url.lower()
            
            if url_lower in social_urls_lower:
                continue
            
            if 't.me' in url_lower or 'wa.me' in url_lower or 'whatsapp' in url_lower:
                continue
            
            return url
        
        return None