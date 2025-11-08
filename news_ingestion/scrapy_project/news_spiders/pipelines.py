import json
import uuid
from datetime import datetime
from minio import Minio
from kafka import KafkaProducer

class MinioKafkaPipeline:
    def __init__(self, minio_config, kafka_config):
        self.minio_client = Minio(
            minio_config["endpoint"],
            access_key=minio_config["access_key"],
            secret_key=minio_config["secret_key"],
            secure=minio_config.get("secure", False),
        )
        self.bucket_name = minio_config["bucket"]
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_config["bootstrap_servers"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        self.topic = kafka_config["topic"]

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            minio_config=crawler.settings.get("MINIO_CONFIG"),
            kafka_config=crawler.settings.get("KAFKA_CONFIG"),
        )

    def process_item(self, item, spider):
        data = dict(item)
        uid = str(uuid.uuid4())
        data["id"] = uid
        data["scraped_at"] = datetime.utcnow().isoformat()

        # Save JSON to MinIO
        file_path = f"raw/onmanorama/{datetime.utcnow().strftime('%Y/%m/%d')}/{uid}.json"
        self.minio_client.put_object(
            self.bucket_name,
            file_path,
            data=json.dumps(data).encode("utf-8"),
            length=len(json.dumps(data).encode("utf-8")),
            content_type="application/json",
        )

        # Send to Kafka
        self.producer.send(self.topic, value=data)
        self.producer.flush()
        return item
    
    def open_spider(self, spider):
        # Initialize connections once
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_config["bootstrap_servers"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def close_spider(self, spider):
        # Cleanup after spider finishes
        if self.producer:
            self.producer.flush()
            self.producer.close()
