import scrapy
from news_spiders.base_spider import BaseNewsSpider
from urllib.parse import urljoin
import feedparser

class OnmanoramaSpider(BaseNewsSpider):
    name = "onmanorama"
    allowed_domains = ["onmanorama.com"]

    RSS_FEEDS = [
        "https://www.onmanorama.com/kerala.feeds.onmrss.xml",
    ]

    def start_requests(self):
        for feed in self.RSS_FEEDS:
            parsed = feedparser.parse(feed)
            for entry in parsed.entries:
                yield scrapy.Request(
                    url=entry.link,
                    callback=self.parse_article,
                    meta={
                        "source": "onmanorama",
                        "title": entry.title,
                        "published_at": entry.get("published", None),
                        "category": "Politics",
                        "language": "en",
                    },
                )
