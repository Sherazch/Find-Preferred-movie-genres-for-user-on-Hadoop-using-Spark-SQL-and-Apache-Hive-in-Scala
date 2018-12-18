
%%init_spark
launcher.master="yarn"
launcher.num_executors=6
launcher.executor_memory="2500m"
launcher.executor_cores="2"
launcher.conf.set("spark.sql.warehouse.dir","/user/hive/warehouse")
launcher.conf.set("spark.sqlcatalogImplementation","hive")


val ratings=sc.textFile("hdfs://C570BD-HM-Master:9000/hadoop-user/data/ratings.dat").map(line=>line.split("#")).
map(splits=>(splits(0),splits(1),splits(2).toInt)).toDF("uid","mid","rat");

ratings.show(5)
ratings.createOrReplaceTempView("ratings")


val movies=sc.textFile("hdfs://C570BD-HM-Master:9000/hadoop-user/data/movies.dat").map(line=>line.split("#")).
map(splits=>(splits(0),splits(2))).flatMapValues(genre=>genre.split("\\|")).toDF("mid","genre");

movies.show(5)
movies.createOrReplaceTempView("movies")

sql("select uid,genre,rat from ratings r, movies m where r.mid=m.mid ").createOrReplaceTempView("data");
sql("select a.uid,a.genre,AVG(a.rat) as avg_rat from data a, data b where a.uid=b.uid AND a.genre=b.genre group by a.uid,a.genre ").
createOrReplaceTempView("data1");
sql(" select uid,genre,avg_rat from(select uid,genre,avg_rat,rank() over (partition by uid order by avg_rat DESC) as r from data1) where r<=5 order by uid,avg_rat DESC").
createOrReplaceTempView("result");

//to write output on a file
val result=sql("select * from result").map(x=>x(0)+" -> "+x(1)+" -> "+x(2));
result.coalesce(1).write.text("file:///home/administrator/Desktop/OutputA4Q1i");


