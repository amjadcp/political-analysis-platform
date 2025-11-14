BOT_NAME = "news_spiders"
SPIDER_MODULES = ["news_spiders"]
NEWSPIDER_MODULE = "news_spiders"
ROBOTSTXT_OBEY = True
DOWNLOAD_DELAY = 2
FEED_EXPORT_ENCODING = "utf-8"
LOG_LEVEL = "INFO"
LOG_ENABLED = True
LOG_FORMAT = "%(levelname)s: %(message)s"
LOG_DATEFORMAT = "%Y-%m-%d %H:%M:%S"


ITEM_PIPELINES = {
    "news_spiders.pipelines.MinioKafkaPipeline": 300,
}

MINIO_CONFIG = {
    "endpoint": "minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "bucket": "kerala-news",
    "secure": False,
}

KAFKA_CONFIG = {
    "bootstrap_servers": ["kafka:9092"],
    "topic": "raw-articles",
}
