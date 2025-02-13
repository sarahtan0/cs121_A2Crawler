import re
from urllib.parse import urlparse, urljoin
from collections import Counter, defaultdict
from bs4 import BeautifulSoup
import atexit
import hashlib
from simhash import Simhash

# Define the maximum allowed content length (in bytes) for a page.
MAX_CONTENT_LENGTH = 1 * 1024 * 1024  # 1MB

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

unique_urls = set()
word_counter = Counter()
subdomain_counts = defaultdict(int)
longest_page = {"url": None, "word_count": 0}
simhashes = []

# Global counter for every page that the crawler sees (downloaded).
total_pages_crawled = 0

current_page_url = None
current_page_text = ""

def scraper(url, resp):
    """
    Crawl and parse the next set of valid links from the current page.
    
    Before extracting outgoing links, this function sets global variables
    for the current page’s URL and textual content.
    """
    global current_page_url, current_page_text, total_pages_crawled
    if resp.status != 200 or resp.raw_response is None:
        return []
    
    # Increment total_pages_crawled as soon as a page is received.
    total_pages_crawled += 1

    # Check if the content is unreasonably large using the Content-Length header if available.
    content_length = resp.raw_response.headers.get("Content-Length")
    if content_length is not None and int(content_length) > MAX_CONTENT_LENGTH:
        print(f"Skipping {url} because content length {content_length} bytes exceeds max allowed size.")
        return []
    # Alternatively, if no Content-Length header is provided, check the actual size of the content.
    if len(resp.raw_response.content) > MAX_CONTENT_LENGTH:
        print(f"Skipping {url} because content size {len(resp.raw_response.content)} bytes exceeds max allowed size.")
        return []
    
    # Parse the page content.
    soup = BeautifulSoup(resp.raw_response.content, "html.parser")
    text = soup.get_text()
    
    # Compute simhash for the current page immediately after download.
    tokens = re.findall(r"[a-zA-Z0-9']+", text.lower())
    tokens = [token for token in tokens if token not in stop_words]
    current_simhash = Simhash(tokens, f=32)
    
    # If a near-duplicate exists, do not add its outgoing links to the frontier.
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
    Decide whether to crawl this URL or not based on various checks.
    
    In addition to the usual URL validations (scheme, file type, domain, etc.),
    if the URL matches the current page (i.e. the page that was just fetched),
    we compute its Simhash (using the simhash library) and check if it is a near-duplicate
    of any previously seen page (using a Hamming distance threshold of 3). If it is, this
    function returns False.
    """
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False
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
        allowed_domains = ("ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu")
        if not any(domain in parsed.netloc for domain in allowed_domains):
            return False

        # Exclude links that contain words implying calendars or dates (e.g., "calendar", "date", "year", etc.).
        # This check now inspects both the path and query.
        combined = parsed.path.lower() + " " + parsed.query.lower()
        if re.search(r"(calendar|date|year|month|day|week|event|seminar|redirect)", combined):
            return False

        return True
    except TypeError:
        print("TypeError for", parsed)
        raise

def is_near_duplicate(simhash_obj):
    """
    Check if the given Simhash object is near-duplicate of any previously seen page.
    (The library’s Simhash objects support a .distance() method.)
    
    A threshold of 3 (or less) is considered near-duplicate.
    """
    for existing in simhashes:
        if simhash_obj.distance(existing) <= 6:
            return True
    return False

def add_unique_url_and_track_content(url, soup):
    """
    Track and store content-related statistics for a URL.
    
    This function is called when a URL is added to the frontier.
    """
    global longest_page
    normalized_url = url.split('#')[0]
    if normalized_url not in unique_urls:
        unique_urls.add(normalized_url)

        parsed = urlparse(url)
        if "ics.uci.edu" in parsed.netloc:
            subdomain_counts[parsed.netloc] += 1

        text = soup.get_text()
        words = re.findall(r'\w+', text.lower())
        filtered_words = [word for word in words if word not in stop_words]

        # Update word frequency and longest page statistics.
        word_counter.update(filtered_words)
        word_count = len(filtered_words)

        if word_count > longest_page["word_count"]:
            longest_page = {"url": url, "word_count": word_count}

def generate_report():
    """
    Generate and print the final crawl report.
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

atexit.register(generate_report)