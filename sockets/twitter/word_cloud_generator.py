from wordcloud import WordCloud
#from pyspark.sql import SparkSession
import nltk
from nltk.corpus import stopwords
import matplotlib.pyplot as plt

f = open("strings.csv",'r')
all_words  = f.read()

nltk.download('stopwords')
#spark = SparkSession.builder.appName("wordcloud_generator").getOrCreate()
#words = spark.read.csv("./csv")

#rows = words.collect()
#all_words = ''
#for row in rows:
    #all_words = all_words + " " + row['_c0']
#print(all_words)
stops = stopwords.words('portuguese')
stops.append('botafogo')

wordcloud = WordCloud(stopwords = stops,
                    background_color = 'black',
                    width = 1920,
                    height= 1080,
                    max_words = 100).generate(all_words)

plt.cla()
plt.imshow(wordcloud)
plt.savefig('figs/wordcloud.png')

