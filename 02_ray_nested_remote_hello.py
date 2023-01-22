from bs4 import BeautifulSoup
import ray
import requests


@ray.remote

def crawl(url, depth=0, maxdepth=2, maxlinks=5):
    
    # prepare arrays for scraped links / futures
    links = []
    link_futures = []

    try:
        # get target url
        f = requests.get(url)
        # add found URL to link array
        links += [url]
        
        # continue following till max depth
        if (depth > maxdepth):
            return links

        # run web scraper on target url
        soup = BeautifulSoup(f.text, 'html.parser')
        i = 0

        # follow links and and find more links
        for link in soup.find_all('a'):
            try:
                i = i+1
                link_futures += [crawl.remote(link["href"], depth=(depth+1), maxdepth=maxdepth)]
                if i > maxlinks:
                    break
            except:
                pass

        # add links and title to array
        for r in ray.get(link_futures):
            links += r

        return links

    except requests.exceptions.InvalidSchema:
        return [] # Skip on invalid links
    except requests.exceptions.MissingSchema:
        return [] # Skip on invalid links

print(ray.get(crawl.remote("https://wiki.instar.com/en/Assistants/Review_Wall/")))
