
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession


exit(0) #FIXME

kafkaServer = "10.0.100.23:9092"
elastic_topic = "tap"
elastic_index = "tap"

sparkConf = SparkConf().set("spark.app.name", "network-tap") \
                        .set("spark.scheduler.mode", "FAIR") \
                        .set("es.nodes", "10.0.100.51") \
                        .set("es.port", "9200") \

sc = SparkContext.getOrCreate(conf=sparkConf)
spark = SparkSession(sc)

df_elastic = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", elastic_topic) \
    .load()

#TODO: importare Elasticsearch e fare i mapping. guarda esempio twitter.

query_elastic = df_elastic.writeStream \
          .option("checkpointLocation", "./checkpoints") \
          .format("es") \
          .start(elastic_index + "/_doc")

        
# Keep running untill terminated
query_elastic.awaitTermination()
