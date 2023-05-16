from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import shutil
"""
"Flamengo esta muito mal".split(" ")
["flamengo", "esta", ...].explode()
[Value]
["flamengo"]
["esta"]
["muiito"]
[o fla,mengo esta bem]
"""

def trata_tweets(df):
    words = df.select(f.explode(f.split(f.lower('value')," ")).alias("words"))\
                .withColumn('words', f.regexp_replace('words', r'http\S+', '' ))\
                .withColumn('words', f.regexp_replace('words', r'@\w+', ''))\
                .withColumn('words', f.regexp_replace('words', r'rt+', ''))\
                .na.replace('', None)\
                .na.drop()                    
    return words

if __name__ == '__main__':
    try:
        for item in ['./check', './csv']:
            shutil.rmtree(item)
    except OSError as error:
        print(error.strerror)

    spark = SparkSession.builder.appName("twitter_client").getOrCreate()
    lines = spark.readStream.format('socket').option('host','localhost')\
                .option('port', 50001).load()
    lines = trata_tweets(lines)
    #query = lines.writeStream.outputMode("append").format("console").start()
    
    query = lines.writeStream.outputMode("append").format("csv")\
        .option("path",'./csv')\
        .option("checkpointLocation","./check")\
        .start()
    
    #word_counts = lines.groupBy('words').count()
    #query = word_counts.writeStream.outputMode("append").format("console").start()
    query.awaitTermination()