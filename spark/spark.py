
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from elasticsearch import Elasticsearch
from pyspark.sql.functions import from_json
import pyspark.sql.types as tp


kafkaServer = "kafkaserver:9092"
elastic_host="elasticsearch"

elastic_topic = "tap"
elastic_index = "tap"

# es_mapping = {
#     "mappings": {
#         "properties": 
#             {
#                 "@timestamp": {"type": "date"},
#                 "hostname": {"type": "text"},
#                 "ip_src":{"type":"ip"},
#                 "port":{"type":"integer"},
#                 "geoip":{
#                     "ip": { "type": "ip" },
#                     "location" : { "type" : "geo_point" },
#                     "latitude" : { "type" : "half_float" },
#                     "longitude" : { "type" : "half_float" },
#                     "country_name":{"type": "text"},
#                     "continent_code" : {"type": "text"},
#                     "country_code2" : {"type": "text"}
#                 }
#             }
#     }
# }

# es = Elasticsearch(hosts=elastic_host) 
# # make an API call to the Elasticsearch cluster
# # and have it return a response:
# response = es.indices.create(
#     index=elastic_index,
#     body=es_mapping,
#     ignore=400 # ignore 400 already exists code
# )

sparkConf = SparkConf().set("spark.app.name", "network-tap") \
                        .set("spark.scheduler.mode", "FAIR") \
                        .set("es.nodes", "http://elasticsearch") \
                        .set("es.port", "9200") \

sc = SparkContext.getOrCreate(conf=sparkConf)
spark = SparkSession(sc)

'''
{
    "ip_src" => "172.20.218.206",
    "geoip" => {
        "country_code2" => "US",
        "continent_code" => "NA",
        "region_name" => "Virginia",
        "location" => {
            "lon" => -78.375,
            "lat" => 36.6534
        },
        "dma_code" => 560,
        "ip" => "65.55.44.109",
        "country_name" => "United States",
        "city_name" => "Boydton"
    },
    "@timestamp" => 2021-07-17T16:55:40.025Z,
    "port" => 443,
    "hostname" => "65.55.44.109"
}
'''

df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", elastic_topic) \
    .load()

network_tap = tp.StructType([
    tp.StructField(name= 'id_str', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'created_at', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'text',       dataType= tp.StringType(),  nullable= True)
])


df_elastic = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", network_tap).alias("data")) \
    .select("data.*")

#TODO: importare Elasticsearch e fare i mapping. guarda esempio twitter.

query_elastic = df_elastic.writeStream \
          .option("checkpointLocation", "./checkpoints") \
          .format("es") \
          .start(elastic_index + "/_doc")

        
# Keep running untill terminated
query_elastic.awaitTermination()
