dataRDD = sc.parallelize([("Leo",34), ("Joao",20), ("Pedro",50)])
agesRDD = (
    dataRDD
    .map(lambda x: (x[0], x[1],x[2]))
    .reduceByKey(lambda x,y: x[0] + y[0], x[1] + y[1] 
    .map(lambda x: (x[0]),x[1][0]/ x[1][1]))
)


data_df = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)], ["name", "age"])
avg_df = data_df.groupBy("name").agg(avg("age"))


mnm_df = (spark
            .read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("data/mnm_dataset.csv"))

mean_df = (mnm_df.select("State","Color","Count")
            .groupBy("State")
            .agg(avg("Count"))
)

mean_color_df = (mnm_df.select("State","Color","Count")
            .groupBy("Color")
            .agg(avg("Count"))
)

from pyspark.sql.types import *
schema = StructType([StructField("author", StringType()),
                    StructField("pages", IntegerType())
])

schema = "`author` STRING, `pages` INT "

///SCALa

import org.apache.spark.sql.types._
val schema = StructType(Array(StructField("author", StringType),
                        (StructField("pages", IntegerType))
))

val schema = "author STRING, pages INT"


