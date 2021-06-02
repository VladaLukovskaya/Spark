from graphframes import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext()  # context
spark = SparkSession(sc)


def float_converter(number):
    try:
        return float(number)
    except:
        return 0.0


def cleaner(row):
    data = row.split('","')
    data[24] = str(float_converter(data[24]))
    return data[0:30]


text = sc.textFile('/home/hadoop/hadoop-3.1.4/tweets/ira_tweets_csv_hashed.csv')
text_without_header = text.filter(lambda row: row.find('"tweetid"') != 0)
clean_text = text_without_header.map(cleaner)

new_DF = spark.createDataFrame(clean_text)
new_DF.createOrReplaceTempView("tw")



# g = GraphFrame(v, e)


