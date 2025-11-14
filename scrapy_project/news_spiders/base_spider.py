import scrapy
from datetime import datetime
from dateutil import parser
from news_spiders.items import NewsArticle

class BaseNewsSpider(scrapy.Spider):
    custom_settings = {"DOWNLOAD_DELAY": 2}

    def parse_article(self, response):
        metadata = response.meta

        article = NewsArticle()
        article["source"] = metadata.get("source")
        article["source_url"] = response.url
        article["title"] = response.xpath("//h1/text()").get() or metadata.get("title")
        article["summary"] = response.xpath("//meta[@name='description']/@content").get()
        article["body"] = " ".join(response.xpath("//p/text()").getall())
        article["category"] = metadata.get("category", "Politics")
        article["language"] = metadata.get("language", "en")
        article["published_at"] = self.parse_date(
            response.xpath("//time/@datetime").get() or metadata.get("published_at")
        )
        article["authors"] = response.xpath("//meta[@name='author']/@content").getall()
        keywords = response.xpath("//meta[@name='keywords']/@content").get()
        article["tags"] = keywords.split(",") if keywords else []
        article["status"] = "raw"

        yield article

    def parse_date(self, date_str):
        if not date_str:
            return None
        try:
            return parser.parse(date_str).isoformat()
        except Exception:
            return datetime.utcnow().isoformat()
