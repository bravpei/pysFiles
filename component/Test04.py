# from pyspark import SparkContext,SparkConf
# import time
# conf=SparkConf().setAppName("test003").setMaster("local[*]")
# sc=SparkContext(conf=conf)
# before=time.time()
# lines=sc.textFile("/home/liupei/test/fiction.txt")
# count=lines.flatMap(eval('lambda s:s.split(" ")')).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[0]).take(20)
# after=time.time()
# print(count)
# print("cost:{}".format(after-before))


# from pyspark import SparkContext
# from pyspark.streaming import StreamingContext
# sc=SparkContext("local[*]","test04")
# ssc=StreamingContext(sc,2)
# lines=ssc.socketTextStream("172.18.130.100",1234)
# words=lines.flatMap(lambda x:x.split(" "))
# pairs=words.map(lambda word:(word,1))
# wordConuts=pairs.reduceByKey(lambda x,y:x+y)
# wordConuts.pprint()
# ssc.start()
# ssc.awaitTermination()



import psycopg2
conn=psycopg2.connect(database="postgres",user="gpadmin",password="gpadmin",host="172.18.130.101",port="5432")
cur=conn.cursor()
try:
    cur.execute("select * from ac_excel_tab")
    conn.commit()
    rows = cur.fetchall()
    print(rows)
except:
    conn.rollback
finally:
    cur.close()
    conn.close()

