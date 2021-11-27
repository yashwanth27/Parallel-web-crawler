import requests
from bs4 import BeautifulSoup
from queue import Queue, Empty,PriorityQueue
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse
import threading
import time
from sklearn.feature_extraction.text import TfidfVectorizer
from scipy.spatial import distance
import numpy as np
import argparse

 
class MultiThreadScraper:
 
    def __init__(self, base_url,query,num,workers):
        self.query = query
        self.base_url = base_url
        self.root_url = '{}://{}'.format(urlparse(self.base_url).scheme, urlparse(self.base_url).netloc)
        self.pool = ThreadPoolExecutor(max_workers=workers)
        self.scraped_pages = set([])
        self.to_crawl = PriorityQueue()
        self.to_crawl.put((1,self.base_url))
        self.counter=0
        self.priority=[]
        self.numOfWebsites = num
 
    def parse_links(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.find_all('a', href=True)
        for link in links:
            url = link['href']
            if url.startswith('/') or url.startswith(self.root_url):
                url = urljoin(self.root_url, url)
                if url not in self.scraped_pages:
                    raw = soup.get_text()
                    corpus=[]
                    corpus.append(self.query)
                    corpus.append(raw)
                    vectorizer = TfidfVectorizer()
                    X = vectorizer.fit_transform(corpus)
                    X = X.toarray()
                    pri = distance.cosine(X[0],X[1])
                    self.to_crawl.put((pri,url))
 
    def scrape_info(self, html):
        return html
 
    def post_scrape_callback(self, res):
        result = res.result()
        if result and result.status_code == 200:
            self.parse_links(result.text)
            self.scrape_info(result.text)
 
    def scrape_page(self, url):
        try:
            res = requests.get(url, timeout=(3, 30))
            #print(threading.current_thread().name,url)
            return res
        except requests.RequestException:
            return
 
    def run_scraper(self):
        while True and len(self.scraped_pages)<self.numOfWebsites: 
            try:
                target = self.to_crawl.get(timeout=60)
                pri = target[0]
                target_url = target[1]
                if target_url not in self.scraped_pages:
                    print("Scraping URL: {} priority {}".format(target_url,pri))
                    self.scraped_pages.add(target_url)
                    self.priority.append(target)
                    job = self.pool.submit(self.scrape_page, target_url)
                    job.add_done_callback(self.post_scrape_callback)
            except Empty:
                return
            except Exception as e:
                print(e)


                continue
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Description of your program')
    parser.add_argument('-n1','--numberOfWorkers', help='numberOfWorkers', required=True)
    parser.add_argument('-n2','--numberOfWebsites', help='numberOfWebsites', required=True)
    args = vars(parser.parse_args())

    numberOfWorkers  = int(args['numberOfWorkers'])
    numberOfWebsites = int(args['numberOfWebsites'])
    
     
    Query = input("QUERY:")
    s = MultiThreadScraper("https://www.amazon.in/",Query,numberOfWebsites,numberOfWorkers)

    start = time.time()
    s.run_scraper()
    print("DONE CRAWLING")
    end = time.time()

    s.priority = sorted(s.priority)
    req_pri = s.priority[0][0]
    for i in s.priority:
        print(i)
        if i[0]>req_pri:
            break
    print(f"{len(s.scraped_pages)} time taken {end-start}")




