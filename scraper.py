import re
from urllib.parse import urlparse, urljoin
from collections import Counter, defaultdict
from bs4 import BeautifulSoup
import atexit
import hashlib
import SimHash

# Stop words to filter out common words that are less informative
stop_words = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at", 
    "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", 
    "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for", 
    "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", 
    "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", 
    "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", 
    "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours", 
    "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", 
    "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", 
    "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", 
    "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", 
    "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", 
    "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", 
    "yourselves"
}

# Data structures for tracking URLs and content information
unique_urls = set()
word_counter = Counter()
subdomain_counts = defaultdict(int)
longest_page = {"url": None, "word_count": 0}
simhashes = []

def scraper(url, resp):
    # Crawl and parse the next set of valid links from the current page
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # Return the list of valid links found on the current page
    if resp.status != 200 or resp.raw_response is None:
        return []

    soup = BeautifulSoup(resp.raw_response.content, "html.parser")
    links = []

    # Extract all anchor tags and normalize URLs
    for a_tag in soup.find_all('a', href=True):
        href = urljoin(url, a_tag['href'])
        normalized_url = href.split('#')[0]  # Remove fragments
        if is_valid(normalized_url):
            add_unique_url_and_track_content(normalized_url, soup)
            links.append(normalized_url)

    return links

def is_valid(url):
    # Decide whether to crawl this URL or not based on various checks
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()):
            return False
        allowed_domains = ("ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu")
        if not any(domain in parsed.netloc for domain in allowed_domains):
            return False
        if len(parsed.query.split('&')) > 5:
            return False
        if re.search(r"(tab_details|do=media|tab_files|do=diff|rev|version|difftype)", parsed.query):
            return False
        if 'doku.php' in parsed.path.lower() and re.search(r"(idx|do=|rev|difftype|media|edit|diff)", parsed.query):
            return False
        if len([segment for segment in parsed.path.split('/') if segment]) > 5:
            return False
        if re.search(r"(/calendar/|/year/|/week/|/month/|/events/|/reply/|/share/|/download/|/attachment/)", parsed.path.lower()):
            return False
        if re.search(r"/wp-content|/upload|/cgi-bin|/admin|/trackback", parsed.path.lower()):
            return False
        if re.search(r"(/-/commit|/-/tree|/-/blame|/-/compare|/-/tags|/-/branches|/-/raw|/-/blob|view=|format=atom)",
                     parsed.path + parsed.query):
            return False
        return True
    except TypeError:
        print("TypeError for ", parsed)
        raise

def calculate_simhash(text):

    # Calculate a 16-bit SimHash for the given text
    words = re.findall(r"[a-zA-Z0-9']+", text.lower())
    bit_vector = [0] * 16

    for word in words:
        # Hash each word using MD5 and fold it down to 16 bits from 128
        word_hash = int(hashlib.md5(word.encode()).hexdigest(), 16)
        folded_hash = 0
        for i in range(16):
            folded_hash ^= (word_hash >> (i * 8)) & 0xFFFF

        for i in range(16):
            bit = (folded_hash >> i) & 1
            if bit == 1:
                bit_vector[i] += 1
            else:
                bit_vector[i] -= 1

    # Generate the SimHash by setting bits based on the bit vector
    simhash = 0
    for i in range(16):
        if bit_vector[i] > 0:
            simhash |= (1 << i)
    return simhash

def distance(hash1, hash2):
    # Calculate the distance between two SimHash values
    x = hash1 ^ hash2
    distance = 0
    while x:
        distance += x & 1
        x >>= 1
    return distance

def is_near_duplicate(simhash, threshold=3):
    # Check if the current SimHash is near-duplicate of previously seen pages
    for existing_simhash in simhashes:
        if distance(simhash, existing_simhash) <= threshold:
            return True
    return False

def add_unique_url_and_track_content(url, soup):
    # Track and store content-related statistics
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

        # Generate SimHash for current page content
        simhash = calculate_simhash(" ".join(filtered_words))
        if is_near_duplicate(simhash):
            print(f"Skipping near-duplicate page: {url}")
            return
        else:
            simhashes.append(simhash)

        # Update word frequency and longest page statistics
        word_counter.update(filtered_words)
        word_count = len(filtered_words)

        if word_count > longest_page["word_count"]:
            longest_page = {"url": url, "word_count": word_count}

def generate_report():
    # Generate and print the final crawl report
    print("\n--- Final Report ---")
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
