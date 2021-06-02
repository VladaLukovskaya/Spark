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
# header_text = text.first().split(',')
text_without_header = text.filter(lambda row: row.find('"tweetid"') != 0)  # transformation
clean_text = text_without_header.map(cleaner)  # transformation
new_DF = spark.createDataFrame(clean_text)  # DataFrame
new_DF.createOrReplaceTempView("tw")

# SparkSQL:
first_request = spark.sql("SELECT _2, count(*) from tw where _5 like '%Russia%' and _12 = 'ru' and _13 like '%Путин%' or _13 like '%Навальный%' or _13 like '%Шойгу%' or _13 like '%Собчак%' or _13 like '%Медведев%' or _13 like '%Мишустин%' or _13 like '%Собянин%' or _13 like '%Жириновский%' group by _2 order by count(*) desc limit 1")
first_request.show()
# first_request.write.format("csv").save('/home/hadoop/hadoop-3.1.4/tweets/first_request')


# RDD:

required_data = clean_text.filter(lambda row: row[1] if 'Russia' in row[4] and row[11] == 'ru'
                                              and 'Путин' in row[12] or 'Навальный' in row[12]
                                              or 'Шойгу' in row[12] or 'Собчак' in row[12]
                                              or 'Медведев' in row[12] or 'Мишустин' in row[12]
                                              or 'Собянин' in row[12] or 'Жириновский' in row[12] else None)
users = required_data.map(lambda x: (x[1], x[0])).countByKey().items()  # is a tuple
rdd_users = sc.parallelize(users)
result = rdd_users.sortBy(lambda x: x[1], False).take(1)
print(result)
# text_file = open("/home/hadoop/hadoop-3.1.4/tweets/RDD-answer", "wt")
# text_file.write(str(result))
# text_file.close()

# map(lambda x: (x[1], x[0])).sortByKey(False).take(10)
