import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import from_json
from pyspark.sql.functions import udf, when
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, BooleanType
from textblob import TextBlob

import json
import requests
import os

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.15.1 pyspark-shell'
os.environ['PYSPARK_LOG_LEVEL'] = 'WARN'

es_endpoint = "https://gqwgzb0w1d:ztnumhbhdu@umbc-search-9870646635.us-east-1.bonsaisearch.net:443"
# es_api_key = "uBh5JpMSQ_SKgyV8GE1eDg"
twitter_schema = StructType([
    StructField("url", StringType(), nullable=True),
    StructField("date", StringType(), nullable=True),
    StructField("content", StringType(), nullable=True),
    StructField("renderedContent", StringType(), nullable=True),
    StructField("id", LongType(), nullable=True),
    StructField("user", StructType([
        StructField("username", StringType(), nullable=True),
        StructField("displayname", StringType(), nullable=True),
        StructField("id", LongType(), nullable=True),
        StructField("description", StringType(), nullable=True),
        StructField("rawDescription", StringType(), nullable=True),
        StructField("descriptionUrls", ArrayType(StringType()), nullable=True),
        StructField("verified", BooleanType(), nullable=True),
        StructField("created", StringType(), nullable=True),
        StructField("followersCount", LongType(), nullable=True),
        StructField("friendsCount", LongType(), nullable=True),
        StructField("statusesCount", LongType(), nullable=True),
        StructField("favouritesCount", LongType(), nullable=True),
        StructField("listedCount", LongType(), nullable=True),
        StructField("mediaCount", LongType(), nullable=True),
        StructField("location", StringType(), nullable=True),
        StructField("protected", BooleanType(), nullable=True),
        StructField("linkUrl", StringType(), nullable=True),
        StructField("linkTcourl", StringType(), nullable=True),
        StructField("profileImageUrl", StringType(), nullable=True),
        StructField("profileBannerUrl", StringType(), nullable=True),
        StructField("url", StringType(), nullable=True)
    ]), nullable=True),
    StructField("outlinks", ArrayType(StringType()), nullable=True),
    StructField("tcooutlinks", ArrayType(StringType()), nullable=True),
    StructField("replyCount", LongType(), nullable=True),
    StructField("retweetCount", LongType(), nullable=True),
    StructField("likeCount", LongType(), nullable=True),
    StructField("quoteCount", LongType(), nullable=True),
    StructField("conversationId", LongType(), nullable=True),
    StructField("lang", StringType(), nullable=True),
    StructField("source", StringType(), nullable=True),
    StructField("sourceUrl", StringType(), nullable=True),
    StructField("sourceLabel", StringType(), nullable=True),
    StructField("media", StringType(), nullable=True),
    StructField("retweetedTweet", StringType(), nullable=True),
    StructField("quotedTweet", StringType(), nullable=True),
    StructField("mentionedUsers", StringType(), nullable=True)
])

es_write_conf = {
    "spark.es.nodes": es_endpoint,
    "spark.es.nodes.wan.only": "true",
    "spark.es.index.auto.create": "true"
}


def analyze_sentiment(text):
    blob = TextBlob(text)
    sentiment_score = blob.sentiment.polarity
    if sentiment_score > 0:
        return "Positive"
    elif sentiment_score < 0:
        return "Negative"
    else:
        return "Neutral"


def extract_hashtags(text):
    hashtags = re.findall(r'#\w+', text)
    return hashtags


def classify_influencer_tier(followers_count, statuses_count):
    thresholds = {
        "tier1": {"followersCount": 100000, "statusesCount": 50000},
        "tier2": {"followersCount": 10000, "statusesCount": 10000},
        "tier3": {"followersCount": 1000, "statusesCount": 1000},
        "tier4": {"followersCount": 100, "statusesCount": 100}
    }

    return (
        when((followers_count >= thresholds["tier1"]["followersCount"]) & (
                statuses_count >= thresholds["tier1"]["statusesCount"]), "Mega")
        .when((followers_count >= thresholds["tier2"]["followersCount"]) & (
                statuses_count >= thresholds["tier2"]["statusesCount"]), "Macro")
        .when((followers_count >= thresholds["tier3"]["followersCount"]) & (
                statuses_count >= thresholds["tier3"]["statusesCount"]), "Micro")
        .when((followers_count >= thresholds["tier4"]["followersCount"]) & (
                statuses_count >= thresholds["tier4"]["statusesCount"]), "Nano")
        .otherwise("User")
    )


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaStreamReader") \
    .getOrCreate()

# Read Kafka messages as a streaming DataFrame
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-12576z.us-west2.gcp.confluent.cloud:9092") \
    .option("subscribe", "raw_tweets") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            "username='DQ3AX66NYROTNC44' "
            "password='8ZB7CLoh3pyhv61bYOxMfhQ+DvYLIOR/TbjlfOsaGbdxJJ4dGZ+NnsAXlvUADFfu';") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert value column from binary to string
value_df = df.select(from_json(col("value").cast("string"), twitter_schema).alias("tweet"))

sentiment_udf = udf(analyze_sentiment, StringType())

hash_tags_udf = udf(extract_hashtags, ArrayType(StringType()))

exploded_df = value_df.selectExpr('tweet.date',
                                  'tweet.renderedContent',
                                  'tweet.id', 'tweet.user',
                                  'tweet.likeCount', 'tweet.quoteCount',
                                  'tweet.retweetCount',
                                  'tweet.replyCount'
                                  )

headers = {
    "Content-Type": "application/json"  # Specify content type as JSON
}


def save_to_elasticsearch(row):
    print(row)
    data = row.asDict()
    document_id = '1'
    # del data["id"]  # Remove the id field from the document
    json_data = json.dumps(data)  # Convert to JSON string
    response = requests.post("https://gqwgzb0w1d:ztnumhbhdu@umbc-search-9870646635.us-east-1.bonsaisearch.net:443/processed/_doc", headers=headers, data=json_data)
    if response.status_code != 201:
        print(f"Failed to save document {document_id} to Elasticsearch: {response.text}")


exploded_df = exploded_df.withColumn("sentiment", sentiment_udf(exploded_df["renderedContent"]))

exploded_df = exploded_df.withColumn("influencer_tier",
                                     classify_influencer_tier(
                                         exploded_df["user.followersCount"],
                                         exploded_df["user.statusesCount"]
                                     ))

exploded_df = exploded_df.withColumn("hashtags", hash_tags_udf(exploded_df["renderedContent"]))

query = exploded_df \
    .writeStream \
    .outputMode("append") \
    .format("org.elasticsearch.spark.sql") \
    .start(**es_write_conf)\
    .option("es.index.auto.create", "true")\
    .save("process_data/_doc")

query.awaitTermination()
