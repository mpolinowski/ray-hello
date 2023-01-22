import ray
import requests

# use list of urls created above
dataset=["https://wiki.instar.com/en/Assistants/Review_Wall/", "https://forum.instar.de", "https://youtu.be/l3EF_JgdGQg", "https://www.youtube.com/about/", "https://www.youtube.com/howyoutubeworks/", "https://www.youtube.com/creators/", "https://www.youtube.com/trends/", "https://blog.youtube/", "https://www.youtube.com/about/press/", "https://www.youtube.com/about/copyright/", "https://youtu.be/Ac1trrZhu9o", "https://www.youtube.com/about/", "https://www.youtube.com/howyoutubeworks/", "https://www.youtube.com/creators/", "https://www.youtube.com/trends/", "https://blog.youtube/", "https://www.youtube.com/about/press/", "https://www.youtube.com/about/copyright/", "https://youtu.be/6N0FATzh1BU", "https://www.youtube.com/about/", "https://www.youtube.com/howyoutubeworks/", "https://www.youtube.com/creators/", "https://www.youtube.com/trends/", "https://blog.youtube/", "https://www.youtube.com/about/press/", "https://www.youtube.com/about/copyright/", "https://youtu.be/2t7Y7I6l6A0", "https://www.youtube.com/about/", "https://www.youtube.com/howyoutubeworks/", "https://www.youtube.com/creators/", "https://www.youtube.com/trends/", "https://blog.youtube/", "https://www.youtube.com/about/press/", "https://www.youtube.com/about/copyright/"]

# and create a dataset from them
urls = ray.data.from_items(dataset)

# download all pages
def get_page(url):
    f = requests.get(url)
    return f.text

# map url dataset and get page content
pages = urls.map(get_page)

# verify that pages where downloaded
# print(pages.take(1))

# split page content into words
words = pages.flat_map(lambda x: x.split(" ")).map(lambda w: (w, 1))
# create GroupedDataset
grouped_words = words.groupby(lambda wc: wc[0])
# take a look at it
print('Generated Data: ', grouped_words)