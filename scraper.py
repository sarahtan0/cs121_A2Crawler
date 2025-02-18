import re
from urllib.parse import urlparse, urljoin
from collections import Counter, defaultdict
from bs4 import BeautifulSoup
import atexit
import hashlib
from simhash import Simhash

# Sets a maximum page size in bytes (multiplication for ease of calculation).
MAX_CONTENT_LENGTH = 2 * 1024 * 1024  # 2MB

# Set of stop words, found in the project description. Added an additional "s" to remove the s from words like "it's"
stop_words = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at", 
    "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", 
    "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for", 
    "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", 
    "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", 
    "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", 
    "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours", 
    "ourselves", "out", "over", "own", "s", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", 
    "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", 
    "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", 
    "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", 
    "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", 
    "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", 
    "yourselves"
}

# Global variables for tracking statistics
unique_urls = set()
word_counter = Counter()
subdomain_counts = defaultdict(int)
longest_page = {"url": None, "word_count": 0}
simhashes = []
total_pages_crawled = 0
current_page_url = None
current_page_text = ""

def scraper(url, resp):
    """
    Processes the current page, but only if its URL contains one of the allowed domains.
    """
    # Check if the URL string contains one of the allowed domains/subdomains.
    allowed_domains = ("ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu")
    if not any(domain in url for domain in allowed_domains):
        print(f"Skipping {url} because it is not in an allowed domain.")
        return []
    
    global current_page_url, current_page_text, total_pages_crawled
    if resp.status != 200 or resp.raw_response is None:
        return []
    
    total_pages_crawled += 1

    # Check the page size before processing
    try:
        content_length = resp.raw_response.headers.get("Content-Length")
        if content_length is not None and int(content_length) > MAX_CONTENT_LENGTH:
            print(f"Skipping {url} because content length {content_length} bytes exceeds maximum size.")
            return []
        if len(resp.raw_response.content) > MAX_CONTENT_LENGTH:
            print(f"Skipping {url} because content size {len(resp.raw_response.content)} bytes exceeds maximum size.")
            return []
    except:
        print("Invalid page content size, skipping")
        return []
        
    # Parse content and compute text
    soup = BeautifulSoup(resp.raw_response.content, "html.parser")
    text = soup.get_text()
    
    # Calculate simhash for duplication
    tokens = re.findall(r"[a-zA-Z]{2,}", text.lower())
    tokens = [token for token in tokens if token not in stop_words]
    current_simhash = Simhash(tokens, f=64)
    
    if is_near_duplicate(current_simhash):
        print(f"Skipping adding links from {url} because its simhash is too similar to a previously seen page.")
        return []
    else:
        simhashes.append(current_simhash)
    
    current_page_url = url
    current_page_text = text
    
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    """
    Extracts and normalizes links from the page while checking for allowed domains.
    """
    if resp.status != 200 or resp.raw_response is None:
        return []
    
    soup = BeautifulSoup(resp.raw_response.content, "html.parser")
    links = []
    
    for a_tag in soup.find_all('a', href=True):
        href = urljoin(url, a_tag['href'])
        normalized_url = href.split('#')[0]  # Remove fragments.
        if is_valid(normalized_url):
            add_unique_url_and_track_content(normalized_url, soup)
            links.append(normalized_url)
    
    return links

def is_valid(url):
    """
    Returns True only if the URL is from one of the allowed domains and passes several other checks.
    """
    # check if the URL string contains an allowed domain.
    allowed_domains = ("ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu")
    if not any(domain in url for domain in allowed_domains):
        return False
    
    try:
        parsed = urlparse(url)
        # Ensure the scheme is either http or https.
        if parsed.scheme not in {"http", "https"}:
            return False
        
        # Filter out URLs ending with unwanted file extensions.
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            r"|png|tiff?|mid|mp2|mp3|mp4"
            r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            r"|epub|dll|cnf|tgz|sha1"
            r"|thmx|mso|arff|rtf|jar|csv"
            r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()):
            return False
        
        # Exclude URLs with certain keywords in their path or query.
        combined = parsed.path.lower() + " " + parsed.query.lower()
        if re.search(r"(calendar|date|year|month|day|week|event|seminar|redirect|filter)", combined):
            return False

        # Exclude URLs with excessively long paths or too many '=' signs.
        if parsed.path.count('/') >= 6:
            return False
        if parsed.path.count('=') >= 4:
            return False

        return True
    except Exception as e:
        print("Invalid url, skipping url:", url, "Error:", e)
        return False

def is_near_duplicate(simhash_obj):
    """
    Checks whether the given simhash is too similar to any already seen.
    """
    for existing in simhashes:
        if simhash_obj.distance(existing) <= 8:
            return True
    return False

def add_unique_url_and_track_content(url, soup):
    """
    Adds a URL to the set of unique URLs and updates word frequency and longest page stats.
    """
    global longest_page
    normalized_url = url.split('#')[0]
    if normalized_url not in unique_urls:
        unique_urls.add(normalized_url)

        parsed = urlparse(url)
        if "ics.uci.edu" in parsed.netloc:
            subdomain_counts[parsed.netloc] += 1

        text = soup.get_text()
        words = re.findall(r"[a-zA-Z]{2,}", text.lower())
        filtered_words = [word for word in words if word not in stop_words]

        word_counter.update(filtered_words)
        word_count = len(filtered_words)

        if word_count > longest_page["word_count"]:
            longest_page = {"url": url, "word_count": word_count}

def generate_report():
    """
    Prints the final crawl report.
    """
    print("\n--- Final Report ---")
    print("Total pages crawled:", total_pages_crawled)
    print("Number of unique pages:", len(unique_urls))

    print("\nLongest page:")
    print(f"URL: {longest_page['url']}, Word Count: {longest_page['word_count']}")

    print("\nTop 50 most common words:")
    for word, count in word_counter.most_common(50):
        print(f"{word} -> {count}")

    print("\nSubdomains within ics.uci.edu:")
    for subdomain, count in sorted(subdomain_counts.items()):
        print(f"{subdomain}, {count}")

# report generation on exit.
atexit.register(generate_report)
