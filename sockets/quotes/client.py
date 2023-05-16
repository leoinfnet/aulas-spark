from pyspark.sql import SparkSession
from pyspark.sql import functions as f
if __name__ == '__main__':
    print("="*50)
    print("Iniciando computação")
    print("="*50)

    spark = SparkSession.builder.appName("quotes_client").getOrCreate()
    lines = spark.readStream.format("socket")\
                .option('host','localhost')\
                .option('port', 50001).load()

    series = lines.select(f.split(lines.value, ":")[0].alias("series"))   
    serie_count = series.groupBy('series').count().sort("count", ascending=False)
    query = serie_count.writeStream.format("console").outputMode("append").trigger(continuous="10 seconds").start()
                
    #query = series.writeStream.outputMode('append').format('console').start()
    query.awaitTermination()    
