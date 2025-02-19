import asyncio
import aiohttp
from bs4 import BeautifulSoup
import logging
from datetime import datetime
from typing import Dict, List, Optional, Set
from urllib.parse import urlencode
import json
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import queue
import threading
from abc import ABC, abstractmethod

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='congress_scraper.log'
)
logger = logging.getLogger(__name__)

@dataclass
class ScrapingJob:
    congress: int
    source: str
    page: int = 1

class SourceScraper(ABC):
    """Base class for different source type scrapers"""
    
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
    
    @abstractmethod
    async def scrape_item(self, url: str) -> Dict:
        """Scrape individual item details"""
        pass
    
    @abstractmethod
    async def parse_search_results(self, html: str) -> List[Dict]:
        """Parse search results page"""
        pass

class LegislationScraper(SourceScraper):
    """Scraper for legislation source type"""
    
    async def parse_search_results(self, html: str) -> List[Dict]:
        soup = BeautifulSoup(html, 'html.parser')
        results = []
        
        # Find all legislation items
        items = soup.find_all('li', class_='expanded')
        
        for item in items:
            try:
                # Extract basic metadata
                title_elem = item.find('h2', class_='item-name')
                if not title_elem:
                    continue
                    
                link = title_elem.find('a')
                bill_number = link.text.strip()
                bill_url = f"https://www.congress.gov{link['href']}"
                
                # Get description and status
                description = item.find('p', class_='item-description')
                title = description.text.strip() if description else "No title available"
                
                status_elem = item.find('span', class_='status')
                status = status_elem.text.strip() if status_elem else "Status unknown"
                
                # Get sponsor
                sponsor_elem = item.find('span', class_='sponsor')
                sponsor = sponsor_elem.text.strip() if sponsor_elem else "No sponsor info"
                
                results.append({
                    'bill_number': bill_number,
                    'title': title,
                    'status': status,
                    'sponsor': sponsor,
                    'url': bill_url,
                    'scraped_at': datetime.now().isoformat()
                })
                
            except Exception as e:
                logger.error(f"Error parsing legislation item: {str(e)}")
                continue
                
        return results
    
    async def scrape_item(self, url: str) -> Dict:
        """Scrape detailed legislation information"""
        try:
            async with self.session.get(url) as response:
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                details = {
                    'url': url,
                    'committees': self._extract_committees(soup),
                    'actions': self._extract_actions(soup),
                    'cosponsors': self._extract_cosponsors(soup),
                    'last_action_date': self._extract_last_action_date(soup),
                    'introduced_date': self._extract_introduced_date(soup),
                    'scraped_at': datetime.now().isoformat()
                }
                
                return details
                
        except Exception as e:
            logger.error(f"Error scraping legislation details from {url}: {str(e)}")
            return {}
    
    def _extract_committees(self, soup) -> List[str]:
        committees = []
        committee_div = soup.find('div', class_='committees')
        if committee_div:
            committee_items = committee_div.find_all('li')
            committees = [item.text.strip() for item in committee_items]
        return committees
    
    def _extract_actions(self, soup) -> List[Dict]:
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
        cosponsors = []
        cosponsor_div = soup.find('div', class_='cosponsors')
        if cosponsor_div:
            sponsor_items = cosponsor_div.find_all('li')
            cosponsors = [item.text.strip() for item in sponsor_items]
        return cosponsors
    
    def _extract_last_action_date(self, soup) -> Optional[str]:
        last_action = soup.find('span', class_='last-action')
        return last_action.text.strip() if last_action else None
    
    def _extract_introduced_date(self, soup) -> Optional[str]:
        introduced = soup.find('span', class_='introduced-date')
        return introduced.text.strip() if introduced else None

class CongressScraper:
    """Main scraper class that coordinates the scraping process"""
    
    def __init__(self, max_workers: int = 5):
        self.base_url = "https://www.congress.gov/search"
        self.max_workers = max_workers
        self.scrapers = {}  # Will hold source type specific scrapers
        
    def _build_search_url(self, congress: int | str, source: str, page: int = 1) -> str:
        """Build search URL with parameters matching congress.gov format"""
        query = {
            "congress": congress if isinstance(congress, str) else str(congress),
            "source": [source]  # Sources are passed as an array in the query
        }
        
        # Handle pagination
        if page > 1:
            query["pageSize"] = 100  # Congress.gov uses pageSize parameter
            query["page"] = page
            
        # Create the JSON structure that congress.gov expects
        params = {
            "q": json.dumps(query, separators=(',', ':'))  # Use compact JSON encoding to match congress.gov
        }
        
        return f"{self.base_url}?{urlencode(params)}"
    
    async def _init_session(self):
        """Initialize aiohttp session with headers"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.session = aiohttp.ClientSession(headers=headers)
        
        # Initialize source-specific scrapers
        self.scrapers['legislation'] = LegislationScraper(self.session)
        # Add other source scrapers here as needed
    
    async def _scrape_page(self, job: ScrapingJob) -> List[Dict]:
        """Scrape a single page of results"""
        url = self._build_search_url(job.congress, job.source, job.page)
        
        try:
            async with self.session.get(url) as response:
                html = await response.text()
                
                # Get source-specific scraper
                scraper = self.scrapers.get(job.source)
                if not scraper:
                    logger.error(f"No scraper found for source type: {job.source}")
                    return []
                
                # Parse search results
                items = await scraper.parse_search_results(html)
                
                # Get details for each item
                detailed_items = []
                for item in items:
                    details = await scraper.scrape_item(item['url'])
                    item.update(details)
                    detailed_items.append(item)
                
                return detailed_items
                
        except Exception as e:
            logger.error(f"Error scraping page {job.page} for congress {job.congress}, source {job.source}: {str(e)}")
            return []
    
    async def scrape(self, start_congress: int = 119, end_congress: int = 115,
                    sources: Set[str] = {'legislation'}) -> List[Dict]:
        """
        Main scraping method that coordinates concurrent scraping
        
        Args:
            start_congress: Most recent congress number to start with
            end_congress: Oldest congress number to scrape
            sources: Set of source types to scrape
        """
        await self._init_session()
        
        # Create job queue
        job_queue = queue.Queue()
        
        # Add initial jobs to queue
        for congress in range(start_congress, end_congress - 1, -1):
            for source in sources:
                job_queue.put(ScrapingJob(congress=congress, source=source))
        
        # Create worker pool
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            while not job_queue.empty():
                job = job_queue.get()
                items = await self._scrape_page(job)
                results.extend(items)
                
                # Check if there are more pages
                if items:  # If we got results, there might be more pages
                    job_queue.put(ScrapingJob(
                        congress=job.congress,
                        source=job.source,
                        page=job.page + 1
                    ))
        
        await self.session.close()
        return results

async def main():
    scraper = CongressScraper(max_workers=5)
    results = await scraper.scrape(
        start_congress=119,
        end_congress=115,
        sources={'legislation'}
    )
    
    # TODO: Add database storage here
    logger.info(f"Scraped {len(results)} items")

if __name__ == "__main__":
    asyncio.run(main())