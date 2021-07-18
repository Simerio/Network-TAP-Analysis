
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from elasticsearch import Elasticsearch
from pyspark.sql.functions import from_json
import pyspark.sql.types as tp


kafkaServer = "kafkaserver:9092"
elastic_host="http://elasticsearch"

elastic_topic = "tap"
elastic_index = "tap"

es_mapping = {
    "mappings": {
        "properties": 
            {
                "@timestamp": {"type": "date"},
                "hostname": {"type": "text"},
                "ip_src":{"type":"ip"},
                "port":{"type":"integer"},
                "geoip":{
                    "ip": { "type": "ip" },
                    "location" : { "type" : "geo_point" },
                    "country_name":{"type": "text"},
                    "continent_code" : {"type": "text"},
                    "country_code2" : {"type": "text"},
                    "dma_code": {"type":"integer"},
                    "region_name": {"type":"text"},
                    "city_name": {"type":"text"}
                }
            }
    }
}


es = Elasticsearch(hosts=elastic_host) 
# make an API call to the Elasticsearch cluster
# and have it return a response:
response = es.indices.create(
    index=elastic_index,
    body=es_mapping,
    ignore=400 # ignore 400 already exists code
)

sparkConf = SparkConf().set("spark.app.name", "network-tap") \
                        .set("spark.scheduler.mode", "FAIR") \
                        .set("es.nodes", "http://elasticsearch") \
                        .set("es.port", "9200") \

sc = SparkContext.getOrCreate(conf=sparkConf)
spark = SparkSession(sc)



df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", elastic_topic) \
    .load()

network_tap = tp.StructType([
    tp.StructField(name= '@timestamp', dataType= tp.DateType(),  nullable= True),
    tp.StructField(name= 'hostname', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'ip_src', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'port', dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'ip', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'location', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'country_name', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'continent_code', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'country_code2', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'dma_code', dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'region_name', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'city_name', dataType= tp.StringType(),  nullable= True)
])




df_elastic = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", network_tap).alias("data")) \
    .select("data.*") \
    .writeStream \
    .format("es") \
    .option("es.nodes", elastic_host).save(elastic_index) \
    .start() \
    .awaitTermination() 

'''
#TODO: importare Elasticsearch e fare i mapping. guarda esempio twitter.

query_elastic = df_elastic.writeStream \
          .option("checkpointLocation", "./checkpoints") \
          .format("es") \
          .start(elastic_index + "/_doc")

        
# Keep running untill terminated
query_elastic.awaitTermination()
'''
