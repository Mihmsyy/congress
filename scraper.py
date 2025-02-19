import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import logging
from typing import Dict, List, Optional

class CongressScraper:
    def __init__(self):
        self.base_url = "https://www.congress.gov"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='congress_scraper.log'
        )
        self.logger = logging.getLogger(__name__)

    def get_recent_bills(self, congress_number: int = 118, limit: int = 20) -> List[Dict]:
        """
        Scrapes recent bills from congress.gov
        
        Args:
            congress_number: The congress session number (default: 118 for current congress)
            limit: Maximum number of bills to retrieve
            
        Returns:
            List of dictionaries containing bill information
        """
        bills = []
        try:
            url = f"{self.base_url}/browse/bills-{congress_number}th-congress/all"
            response = self.session.get(url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            bill_items = soup.find_all('li', class_='expanded')
            
            for item in bill_items[:limit]:
                bill_info = self._parse_bill_item(item)
                if bill_info:
                    bills.append(bill_info)
                    self.logger.info(f"Successfully parsed bill: {bill_info['bill_number']}")
                
                # Respect rate limiting
                time.sleep(1)
                
        except Exception as e:
            self.logger.error(f"Error scraping bills: {str(e)}")
            
        return bills

    def _parse_bill_item(self, item_soup) -> Optional[Dict]:
        """
        Parses individual bill information from HTML
        """
        try:
            # Extract basic bill information
            title_elem = item_soup.find('h2', class_='item-name')
            if not title_elem:
                return None
                
            bill_link = title_elem.find('a')
            bill_number = bill_link.text.strip()
            bill_url = self.base_url + bill_link['href']
            
            # Get bill title and status
            description = item_soup.find('p', class_='item-description')
            title = description.text.strip() if description else "No title available"
            
            status_elem = item_soup.find('span', class_='status')
            status = status_elem.text.strip() if status_elem else "Status unknown"
            
            # Get sponsor information
            sponsor_elem = item_soup.find('span', class_='sponsor')
            sponsor = sponsor_elem.text.strip() if sponsor_elem else "No sponsor info"
            
            return {
                'bill_number': bill_number,
                'title': title,
                'status': status,
                'sponsor': sponsor,
                'url': bill_url,
                'scraped_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error parsing bill item: {str(e)}")
            return None

    def get_bill_details(self, bill_url: str) -> Optional[Dict]:
        """
        Scrapes detailed information about a specific bill
        
        Args:
            bill_url: Full URL to the bill's page
            
        Returns:
            Dictionary containing detailed bill information
        """
        try:
            response = self.session.get(bill_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract detailed information
            details = {
                'url': bill_url,
                'full_text': self._extract_full_text(soup),
                'committees': self._extract_committees(soup),
                'actions': self._extract_actions(soup),
                'cosponsors': self._extract_cosponsors(soup),
                'scraped_at': datetime.now().isoformat()
            }
            
            return details
            
        except Exception as e:
            self.logger.error(f"Error getting bill details for {bill_url}: {str(e)}")
            return None

    def _extract_full_text(self, soup) -> str:
        """Extracts bill's full text if available"""
        text_div = soup.find('div', class_='generated-text-container')
        return text_div.text.strip() if text_div else "Full text not available"

    def _extract_committees(self, soup) -> List[str]:
        """Extracts committee information"""
        committees = []
        committee_div = soup.find('div', class_='committees')
        if committee_div:
            committee_items = committee_div.find_all('li')
            committees = [item.text.strip() for item in committee_items]
        return committees

    def _extract_actions(self, soup) -> List[Dict]:
        """Extracts bill actions/timeline"""
        actions = []
        action_table = soup.find('table', class_='actions')
        if action_table:
            rows = action_table.find_all('tr')[1:]  # Skip header
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    actions.append({
                        'date': cols[0].text.strip(),
                        'action': cols[1].text.strip()
                    })
        return actions

    def _extract_cosponsors(self, soup) -> List[str]:
        """Extracts cosponsor information"""
        cosponsors = []
        cosponsor_div = soup.find('div', class_='cosponsors')
        if cosponsor_div:
            sponsor_items = cosponsor_div.find_all('li')
            cosponsors = [item.text.strip() for item in sponsor_items]
        return cosponsors

    def save_to_csv(self, bills: List[Dict], filename: str = 'bills.csv'):
        """Saves scraped bill information to CSV"""
        try:
            df = pd.DataFrame(bills)
            df.to_csv(filename, index=False)
            self.logger.info(f"Successfully saved {len(bills)} bills to {filename}")
        except Exception as e:
            self.logger.error(f"Error saving to CSV: {str(e)}")

def main():
    scraper = CongressScraper()
    
    # Get recent bills
    bills = scraper.get_recent_bills(limit=10)
    
    # Get detailed information for each bill
    for bill in bills:
        details = scraper.get_bill_details(bill['url'])
        if details:
            bill.update(details)
    
    # Save results
    scraper.save_to_csv(bills, 'congress_bills.csv')

if __name__ == "__main__":
    main()