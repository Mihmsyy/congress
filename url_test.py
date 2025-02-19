from congress_scraper import CongressScraper
from urllib.parse import unquote
import json

def test_url_builder():
    scraper = CongressScraper()
    
    # Test cases
    test_cases = [
        {
            'congress': 119,
            'source': 'legislation',
            'page': 1,
            'description': 'Basic legislation search'
        },
        {
            'congress': 'all',
            'source': 'legislation',
            'page': 1,
            'description': 'All congress legislation search'
        },
        {
            'congress': 118,
            'source': 'comreports',
            'page': 2,
            'description': 'Committee reports with pagination'
        }
    ]
    
    for case in test_cases:
        url = scraper._build_search_url(case['congress'], case['source'], case['page'])
        print(f"\nTest: {case['description']}")
        print(f"Generated URL: {url}")
        
        # Decode the URL to make it readable
        decoded_url = unquote(url)
        print(f"Decoded URL: {decoded_url}")
        
        # Parse and pretty print the query parameters
        q_param = decoded_url.split('?q=')[1]
        query_json = json.loads(q_param)
        print("\nQuery parameters:")
        print(json.dumps(query_json, indent=2))
        print("-" * 80)

if __name__ == "__main__":
    test_url_builder()