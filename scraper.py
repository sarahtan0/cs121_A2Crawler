import re
from urllib.parse import urlparse, urljoin
from collections import Counter, defaultdict
from bs4 import BeautifulSoup
import atexit

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

unique_urls = set()
word_counter = Counter()
subdomain_counts = defaultdict(int)
longest_page = {"url": None, "word_count": 0}

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # return empty if the response is not 200 or is None
    if resp.status != 200 or resp.raw_response is None:
        return []

    soup = BeautifulSoup(resp.raw_response.content, "html.parser")

    links = []
    for a_tag in soup.find_all('a', href=True):
        # connect all the relative urls found with the base url of the site
        href = urljoin(url, a_tag['href'])

        # remove fragments for normalizing
        normalized_url = href.split('#')[0]

        # only appends valid urls
        if is_valid(normalized_url):
            links.append(normalized_url)

    return links

    # return list()

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
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
        # specify the domains allowed as per the project
        allowed_domains = (
            "ics.uci.edu",
            "cs.uci.edu",
            "informatics.uci.edu",
            "stat.uci.edu"
        )


         # verify that the domain is in the allowed domains
        if not any(domain in parsed.netloc for domain in allowed_domains):
            return False

        # do not crawl if there are too many query parameters (could be a sign of a trap)
        if len(parsed.query.split('&')) > 5:
            return False

        #queries that look like traps
        if re.search(r"(tab_details|do=media|tab_files|do=diff|rev|version|difftype)", parsed.query):
            return False

        #block certain urls in doku.php
        if 'doku.php' in parsed.path.lower() and re.search(r"(idx|do=|rev|difftype|media|edit|diff)", parsed.query):
            return False

        #split path and filter out empty segments
        path_segments = [segment for segment in parsed.path.split('/') if segment]
        if len(path_segments) > 5:
            return False

        # normalizes the path to lowercase and checks if it contains any url's that typically lead to traps or
        # sites with minimal information
        if re.search(r"(calendar|year|week|month|events|reply|share|download|attachment)", parsed.path.lower()):
            return False

        # checsk for links that commonly lead to pages with large files or pages that require admin access
        if re.search(r"/wp-content|/upload|/cgi-bin|/admin|/trackback", parsed.path.lower()):
            return False

        return True



    except TypeError:
        print ("TypeError for ", parsed)
        raise

def add_unique_url_and_track_content(url, soup):
    global longest_page

    # remove fragment
    normalized_url = url.split('#')[0]
    if normalized_url not in unique.urls:
        unique_urls.add(normalized_url)

        parsed = urlparse(url)
        if "ics.uci.edu" in parsed.netloc:
            subdomain_counts[parsed.netloc] += 1

        text = soup.get_text()
        words = re.findall(r'\w+', text.lower())
        filtered_words = [word for word in words if word not in stop_words]

        #adds one to each word in word_counter
        word_counter.update(filtered_words)

        word_count = len(filtered_words)
        if word_count > longest_page["word_count"]:
            longest_page = {"url": url, "word_count": word_count}

def generate_report():
    print("\n--- Final Report ---")
    print("Number of unique pages:", len(unique_urls))

    print("\nLongest page:")
    print(f"URL: {longest_page['url']}, Word Count: {longest_page['word_count']}")

    print("\nTop 50 most common words:"):
    for word, count in word_counter.most_common(50):
        print(f"{word} -> {count}")

    print("\nSubdomains within ics.uci.edu:")
    for subdomain, count in sorted(subdomain_counts.items()):
        print(f"{subdomain}, {count}")

atexit.register(generate_report)