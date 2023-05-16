from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName('kinesis').getOrCreate()
    print("="*50)
    print("Iniciando computação")
    print("="*50)

    kinesis = spark.readStream\
                .format('kinesis')\
                .option('streamName', 'aula_spark')\
                .option('endpointUrl', 'https://kinesis.us-east-2.amazonaws.com')\
                .option('region', 'us-east-2')\
                .option('awsAccessKeyId','XXXXXXXXXXXXXXXXXXXXXXXXXX')\
                .option('awsSecretKey', 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX' )\
                .option('startingposition','TRIM_HORIZON')\
                .load()
    #Processar StreamDS 
    #[id,randon_numer]
    ds.groupBy('cliend').avg('random_number')
            