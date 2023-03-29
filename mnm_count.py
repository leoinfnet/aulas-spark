import sys
from pyspark.sql import Session

if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print("Uso do programa: mnm_count <file> <State> ", file = sys.stderr)
        sys.exit(-1)
spark = (
    SparkSession
    .builder
    .appName("MNM Counter")
    .getOrCreate())

mnm_file = sys.argv[1]
state = sys.argv[2]
mnm_df = (spark
            .read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(mnm_file))
count_mnm_df = (mnm_df
    .select("State", "Color","Count")
    .groupBy("State", "Color")
    .sum("Count")
    .orderBy("sum(Count)", ascending=False)
)
count_mnm_df.show(n=30, truncate=False)

ca_state_count_mnm_df = (mnm_df.select("State", "Color","Count")
    .where(mnm_df.State ==  state)
    .groupBy("State", "Color")
    .sum("Count")
    .orderBy("sum(Count)", ascending=False)
)

tx_state_count_mnm_df = (mnm_df.select("State", "Color","Count")
    .where(mnm_df.State ==  'TX')
    .groupBy("State", "Color")
    .sum("Count")
    .orderBy("sum(Count)", ascending=False)
)
tx_state_count_mnm_df.show()

spark.stop()




