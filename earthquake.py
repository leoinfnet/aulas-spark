import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
import folium
from datetime import datetime
def get_alertas(df):
    print("Alertas : ")
    df.select("alert").groupBy("alert").count().show()

def terremtos_por_anos(df):
    print("Terremotos por Anos: ")
    df.select("*", year("datetime")).groupBy(year("datetime")).count().orderBy("count", ascending=False).show()
def terremotos_por_meses_do_ano(df,ano):
    print("Teremotos por mes do ano: ")
    df_by_year = df.select("*", year("datetime")).where(year("datetime") == ano)

    #(df_by_year.select("*", month("datetime")).groupBy(month("datetime")).count().orderBy(month("datetime")).show())    
def terremotos_por_hora(df):
    print("Terremotos por hora: ")
    (df.select("*", hour("datetime")).groupBy(hour("datetime")).count().orderBy(hour("datetime")).show(30))
def terremotos_por_pais(df):
    print("Terremoto por pais: ")
    df.groupBy("country").count().orderBy("count", ascending=False).show()
def agregacoes_por_pais(df):
    print("AGG por paises: ") 
    return df.groupBy("country").agg(F.avg("magnitude"), F.max("magnitude"), F.min("magnitude"), F.stddev("magnitude")).orderBy("avg(magnitude)", ascending=False)

def agregacoes_por_pais_especifico(df, pais):
    print("Calculando AGG de " + pais)
    return df.filter(col("country") == pais).agg(F.avg("magnitude"), F.max("magnitude"), F.min("magnitude"), F.stddev("magnitude"))
def pais_com_maior_terremoto(df):
    print("Pais com maior Terremoto")
    df.groupBy("country").agg(F.max("magnitude")).orderBy("max(magnitude)", ascending=False).show()
def generate_pie_chart(df):
    rows = (df.groupBy("country").count().orderBy("count", ascending=False).head(7))
    rows.pop(0)
    countryNames = []
    countryValues = []
    for row in rows:
        countryNames.append(row.country)
        countryValues.append(row["count"])
    fig, ax = plt.subplots()
    ax.pie(countryValues, labels=countryNames, shadow=True, startangle=90)
    plt.savefig("figs/pie.png")
    plt.cla()
    plt.clf()
def generate_bar_chart(df):
    horasKeys = []
    horasValues = []
    horas = (df.select("*", hour("datetime")).groupBy(hour("datetime")).count().orderBy(hour("datetime")).head(30))
    for hora in horas:
        horasKeys.append(row["hour(datetime)"])
        horasValues.append(row["count"])
    fix , ax = plt.subplots()
    ax.bar(horasKeys, horasValues)
    plt.savefig("figs/bars.png")
def plot_markers_in_map(df,pais):
    print("Gerando mapa para o " + pais)
    pais_df = (df.select("magnitude","datetime","latitude", "longitude" ).where(col("country") == pais))
    latitudes = pais_df.select(F.collect_list("latitude")).first()[0]
    longitudes = pais_df.select(F.collect_list("longitude")).first()[0]
    magnitudes = pais_df.select(F.collect_list("magnitude")).first()[0]

    map = folium.Map()
    map = folium.Map(location=[latitudes[0], longitudes[0]], zoom_start=6)
    for marker in range(0, pais_df.count()):
        map.add_child(folium.Marker(location=[latitudes[marker], longitudes[marker] ], poppup=magnitudes[marker])) 
    path = "figs/"
        
    map.save(path + pais + ".html")
def save_to_database(df):
    tempo_geracao = datetime.now()
    df_novo = df
    df_novo = df_novo.withColumn("datetime", lit(tempo_geracao))
    (
        df.write.format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", "jdbc:mysql://database-1.cihuxrkjd5ya.sa-east-1.rds.amazonaws.com:3306/earthquake")
        .option("dbtable", "historico")
        .option("user", "admin")
        .option("password", "12345678")
        .save()
    )


if __name__ == "__main__":
    file = open("outs/report.txt", "w")
    spark = (
        SparkSession.builder
        .appName("Earthquake reports")
        .getOrCreate()
    )
    df = (
        spark.read.format("jdbc")
        .mode("append")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", "jdbc:mysql://database-1.cihuxrkjd5ya.sa-east-1.rds.amazonaws.com:3306/earthquake")
        .option("dbtable", "earthquake")
        .option("user", "admin")
        .option("password", "12345678")
        .load()
    )
    pais = sys.argv[1]
    print("Iniciando Programa...")

    df = df.withColumn("datetime", to_timestamp("date_time", "dd-MM-yyyy HH:mm"))
    df = df.drop("date_time")


    #get_alertas()
    agregacoes_por_pais_especifico(df,pais).show()
    plot_markers_in_map(df, pais)
    
    aggs_por_paises = agregacoes_por_pais(df)

    print("Salvando em csv")
    #aggs_por_paises.write.mode("overwrite").parquet("outs/parquet")
    #aggs_por_paises.write.format("csv").mode('overwrite').options(header=True, delimiter=";").save("outs/aggs.csv")
    file.write("Saida em python \n")
    #file.write(aggs_por_paises.show())
    save_to_database(aggs_por_paises)
