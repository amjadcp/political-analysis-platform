import scrapy

class NewsArticle(scrapy.Item):
    id = scrapy.Field()
    source = scrapy.Field()
    source_url = scrapy.Field()
    title = scrapy.Field()
    summary = scrapy.Field()
    body = scrapy.Field()
    category = scrapy.Field()
    language = scrapy.Field()
    published_at = scrapy.Field()
    scraped_at = scrapy.Field()
    authors = scrapy.Field()
    tags = scrapy.Field()
    status = scrapy.Field()
