from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from graphframes import *
import plotly.express as px
import plotly.graph_objects as go
def get_page_rank(g):
    results = g.pageRank(resetProbability=0.15, maxIter=20)
    return results

def get_more_relevant(results ,n=20):
    print("*******  Aeroportos Mais Relevantes segundo Page Rank  ************")
    relevants = results.vertices.select("id","pagerank").sort("pagerank", ascending=False)
    relevants.show(n)
    relevants_matrix = relevants.collect()
    labels = []
    keys = []
    for x in range(n):
        labels.append(relevants_matrix[x][0])
        keys.append(relevants_matrix[x][1])
    fig = px.bar(x = labels, y= keys)
    fig.write_html("figs/more_relevant.html", auto_open=False)
    print("******************************")

def airport_with_max_arrives(g):
    print("*******  Airports  ************")
    max_in_degree = g.inDegrees.groupBy().max('inDegree').collect()[0][0]
    airport = g.inDegrees.where(col("inDegree") == lit(max_in_degree)).collect()[0][0]
    print(f'Aeroporto com mais chegadas: {airport} com {max_in_degree} chegadas')


def get_maior_delay(g):
    print("*******  Delay ************")
    print(f' Maior Delay: {g.edges.groupBy().max("delay").collect()[0][0]} ')
    print("******************************")

def delayed_or_in_time(g):
    print("*******  Qualidade dos Voos ************")
    print(f' Voos no horario: { g.edges.filter("delay = 0").count()} ')
    print(f' Voos adiantados: { g.edges.filter("delay < 0").count()} ')
    print(f' Voos atrasados: {  g.edges.filter("delay > 0").count()} ')
    print("******************************")
def have_connection_motif(g, src, dst):
    print("*******  Caminho Minimo com Motif ************")
    g.find("(a) -[ab] -> (b)").filter(f'a.id = \'{src}\' and  b.id = \'{dst}\'').show()
    print("******************************")

def have_connection(g,src,dst, mathPath):
    print("*******  Caminho Minimo com BFS ************")
    path = g.bfs(f'id == {src}', f'id == {dst}', maxPathLength=mathPath)
    path.show()
    print("******************************")
def get_most_delayed_flies(g,airport ='SFO', n=20):
    print("*******  Maior Media de atrasos entre aeroportos ************")
    most_delayed=  g.edges.filter("src = '" + airport + "' and delay > 0").groupBy("src","dst").avg("delay").sort("avg(delay)",ascending=False)
    most_delayed_list = most_delayed.collect()
    labels = []
    keys = []
    for x in range(n):
        labels.append(f'{most_delayed_list[x][0]}|{most_delayed_list[x][1]}')
        keys.append(most_delayed_list[x][2])
    
    fig = px.bar(x = labels, y= keys)
    fig.write_html("figs/more_delayed.html", auto_open=False)
def plot_more_relevants_airports(results,g, n=20):
    print("*******  Gerando Mapa com aeroportos mais relevantes ************")
    most_relevants = results.vertices.select("id","pagerank").sort("pagerank",ascending=False).collect()
    airports = []
    for x in range(n):
        name = most_relevants[x][0]
        rel =  most_relevants[x][1]
        lat = g.vertices.filter(f'id= \'{name}\'').collect()[0][3]
        long = g.vertices.filter(f'id= \'{name}\'').collect()[0][4]
        airport = {'name': name, 'rel': rel, 'lat': lat, 'long': long}
        airports.append(airport)
    #print(airports)
    fig = go.Figure(go.Scattermapbox(
            mode = "markers+lines",
            lon = [airports[0]['long']],
            lat = [airports[0]['lat'] ],
            name = airports[0]['name'],
            marker = {'size': 30}))
    for x in range(len(airports[1:])):
        fig.add_trace(go.Scattermapbox(
            mode = "markers",
            lon = [airports[x]['long']],
            lat = [airports[x]['lat']],
            name = airports[x]['name'],
            marker = {'size': 30}))

    fig.update_layout(
        margin ={'l':0,'t':0,'b':0,'r':0},
        mapbox = {
            'center': {'lon':-122.375, 'lat': 37.61899948120117},
            'style': "stamen-terrain",
            'center': {'lon': -122.375, 'lat':37.61899948120117},
            'zoom': 3})
    fig.write_html('figs/plotair.html', auto_open=False)





if __name__ == "__main__" :
    spark = (
        SparkSession.builder
        .appName("Airport Analist")
        .getOrCreate()
    )

    airports = spark.read.format("csv").options(header= 'true', inferSchema='true').load("data/airports.csv")
    trips = spark.read.format("csv").options(header= 'true', inferSchema='true').load("data/departuredelays.csv")

    vertex = airports
    vertex = vertex.drop("ID")
    vertex = vertex.withColumn("id", col("IATA"))
    vertex = vertex.drop("ICAI","IATA","TZ","DST", "ALTITUDE","TIMEZONE","TYPE","SOURCE")
    vertex = vertex.dropDuplicates(["id"])

    g = GraphFrame(vertex,trips)
    print("*******  Iniciando Analise ************")
    print("Grafo Carregado")
    print(f' Total de Aeroportos: {g.vertices.count()} ')
    print(f' Total de Rotas: {g.edges.count()} ')
    print("Executando Page Rank")
    print("***************************************")
    results = get_page_rank(g)
    #get_maior_delay(g)
    #delayed_or_in_time(g)
    #airport_with_max_arrives(g)
    #get_more_relevant(results,30)
    #get_most_delayed_flies(g,"JFK",20)
    #have_connection(g,"JFK", "ALB",2)
    plot_more_relevants_airports(results=results,g=g,n=20)
