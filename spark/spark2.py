
from pprint import pprint
from pyspark.sql.functions import udf, unix_timestamp, window
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from elasticsearch import Elasticsearch
from pyspark.sql.functions import from_json
import pyspark.sql.types as tp
import time
from sklearn.linear_model import LinearRegression
from ipwhois import IPWhois
import pandas as pd
import numpy as np

kafkaServer = "kafkaserver:9092"
elastic_host = "elasticsearch"

elastic_topic = "tap"
elastic_index = "tap"

es_mapping = {
    "mappings": {
        "properties": {
            "@timestamp": {"type": "date", "format": "epoch_second"},
            "ip_src": {"type": "ip"},
            "geoip": {
                "properties": {
                    "ip": {"type": "ip"},
                    "location": {"type": "geo_point"}
                }
            }
        }
    }
}
cache = {}


def get_linear_regression_model(df: pd.DataFrame):
    x = df['timestamp'].to_numpy()
    y = np.array([df.shape[0]])
    lr = LinearRegression()
    lr.fit(x, y)
    return lr


@udf
def getOwner(ip):
    # pprint(ip)
    if ip not in cache:
        res = IPWhois(ip).lookup_whois()
        owner = res["nets"][0]["description"]
        if owner is None:
            owner = res["nets"][0]["name"]
        # print(owner)
        cache[ip] = owner

    return cache[ip]


es = Elasticsearch(hosts=elastic_host)
while not es.ping():
    time.sleep(1)

response = es.indices.create(
    index=elastic_index,
    body=es_mapping,
    ignore=400  # ignore 400 already exists code
)

sparkConf = SparkConf().set("spark.app.name", "network-tap") \
    .set("es.nodes", elastic_host) \
    .set("es.port", "9200") \

sc = SparkContext.getOrCreate(conf=sparkConf)
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("WARN")

df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", elastic_topic) \
    .option("startingOffset", "earliest") \
    .load()

location_struct = tp.StructType([
    tp.StructField(name='lat', dataType=tp.StringType(),  nullable=True),
    tp.StructField(name='lon', dataType=tp.StringType(),  nullable=True)
])
geoip_struct = tp.StructType([
    tp.StructField(name='ip', dataType=tp.StringType(),  nullable=True),
    tp.StructField(name='location', dataType=location_struct,  nullable=True),
    tp.StructField(name='country_name',
                   dataType=tp.StringType(),  nullable=True),
    tp.StructField(name='continent_code',
                   dataType=tp.StringType(),  nullable=True),
    tp.StructField(name='country_code2',
                   dataType=tp.StringType(),  nullable=True),
    tp.StructField(name='dma_code', dataType=tp.IntegerType(),  nullable=True),
    tp.StructField(name='region_name',
                   dataType=tp.StringType(),  nullable=True),
    tp.StructField(name='city_name', dataType=tp.StringType(),  nullable=True)
])

network_tap = tp.StructType([
    tp.StructField(name='@timestamp',
                   dataType=tp.StringType(),  nullable=True),
    tp.StructField(name='hostname', dataType=tp.StringType(),  nullable=True),
    tp.StructField(name='ip_src', dataType=tp.StringType(),  nullable=True),
    tp.StructField(name='port', dataType=tp.IntegerType(),  nullable=True),
    tp.StructField(name='geoip', dataType=geoip_struct, nullable=True)
])


def get_output_df():
    return pd.DataFrame(columns=[
        '@timestamp'
    ])

def predict_value(model, milliseconds):
    rssi_range = lambda s: max(min(0, s), -120)
    s = model.predict([[milliseconds]])[0]
    return rssi_range(s)


def predict(df: pd.DataFrame) -> pd.DataFrame:
    def nrows(df): return df.shape[0]
    newdf = get_output_df()
    if (nrows(df) < 1):
        return newdf
    model = get_linear_regression_model(df)
    lastPacketMillisec = df['timestamp'].values.max()
    next_minutes = [ (lastPacketMillisec + (60000 * i)) for i in range(5) ]
    next_bandwitch = [predict_value(model, m) for m in next_minutes]
    for millis, rssi in zip(next_minutes, next_bandwitch):
        newdf = newdf.append(make_series(df, millis, rssi), ignore_index=True)
    return newdf


df_kafka = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", network_tap).alias("data"))\
    .select("data.*").\
    withColumn("timestamp", unix_timestamp(
        '@timestamp', format="yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

df_kafka = df_kafka.withColumn("Owner", getOwner(df_kafka.geoip.ip))

win = window(df_kafka.timestamp, "1 minutes")
df_kafka = df_kafka\
    .groupBy("timestamp", win)\
    .applyInPandas(predict, network_tap)


df_kafka = df_kafka\
    .writeStream \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .format("es") \
    .start(elastic_index) \
    .awaitTermination()
