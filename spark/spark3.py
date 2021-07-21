
from pprint import pprint
from pyspark.sql.functions import udf, unix_timestamp, window
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from elasticsearch import Elasticsearch
from pyspark.sql.functions import from_json, col, to_timestamp, unix_timestamp, window
import pyspark.sql.types as tp
import time
from sklearn.linear_model import LinearRegression
from ipwhois import IPWhois
import pandas as pd


pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


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

es_mapping2 = {
    "mappings": {
        "properties": {
            "time": {"type": "date", "format": "epoch_second"}
        }
    }
}
cache = {}


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
es.indices.create(
    index=elastic_index,
    body=es_mapping,
    ignore=400  # ignore 400 already exists code
)

es.indices.create(
    index=elastic_index+"-netstat",
    body=es_mapping2,
    ignore=400  # ignore 400 already exists code
)

def get_resulting_df_schema():
    return  tp.StructType() \
        .add("time",            tp.StringType()) \
        .add("count",           tp.IntegerType()) \
        .add("predict",         tp.IntegerType())


def get_linear_regression_model(df: pd.DataFrame):
    x = df['time'].to_numpy().reshape(-1, 1)
    y = df['count'].to_numpy()
    lr = LinearRegression()
    lr.fit(x, y)
    return lr 

def get_output_df():
    return pd.DataFrame(columns=[
        'time', 
        'count', 
        'predict'
    ])

def make_series(df, timestamp, predicted_rssi) -> pd.Series:
    return pd.Series([
        str(timestamp),
        df.iloc[0]['count'],
        int(predicted_rssi)
    ], index=[
        'time', 
        'count', 
        'predict'
    ])

def predict_value(model, milliseconds):
    s = model.predict([[milliseconds]])[0]
    return s
    

def predict(df: pd.DataFrame) -> pd.DataFrame:
    nrows = lambda df: df.shape[0]    
    newdf = get_output_df()
    n = nrows(df)
    print(n)
    if (n < 1):
        return newdf
    model = get_linear_regression_model(df.tail(5))

    lastSignalMillisec = df['time'].values.max()
    next_minutes = [ (lastSignalMillisec + (30 * i)) for i in range(5) ]
    next_rssi = [predict_value(model, m) for m in next_minutes]
    print(next_rssi)
    for millis, rssi in zip(next_minutes, next_rssi):
        newdf = newdf.append(make_series(df, millis, rssi), ignore_index=True)
    
    return newdf

sparkConf = SparkConf().set("spark.app.name", "network-tap") \
    .set("es.nodes", elastic_host) \
    .set("es.port", "9200") \

sc = SparkContext.getOrCreate(conf=sparkConf)
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("ERROR")

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
                   dataType=tp.TimestampType(),  nullable=True),
    tp.StructField(name='hostname', dataType=tp.StringType(),  nullable=True),
    tp.StructField(name='ip_src', dataType=tp.StringType(),  nullable=True),
    tp.StructField(name='port', dataType=tp.IntegerType(),  nullable=True),
    tp.StructField(name='geoip', dataType=geoip_struct, nullable=True)
])

df_kafka = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", network_tap).alias("data"))\
    .select("data.*")
df_kafka = df_kafka.withColumn("Owner", getOwner(df_kafka.geoip.ip))

def funny_fun(ddd):
    for d in ddd:
        yield predict(d)

counting = df_kafka\
    .select("@timestamp")\
    .withWatermark("@timestamp","30 seconds")\
    .groupBy(window("@timestamp", "30 seconds"))\
    .count()\
    .select("count",unix_timestamp(col("window.start")).alias("time"))\
    .mapInPandas(funny_fun,get_resulting_df_schema())
#    .applyInPandas(predict,get_resulting_df_schema())


df_kafka\
    .writeStream\
    .option("checkpointLocation", "/tmp/checkpoints") \
    .format("es") \
    .start(elastic_index) \

counting\
    .writeStream \
    .option("checkpointLocation", "/tmp/checkpoints2") \
    .format("es") \
    .start(elastic_index+"-netstat") \
    .awaitTermination()

