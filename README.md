# Find-Preferred-movie-genres-for-user-on-Hadoop-using-Spark-SQL-and-Apache-Hive-in-Scala
Finding the most preferred movie genres for every user


 We have to user Spark SQL and write a spark standalone program (.scala or .py)  that takes a movie rating dataset and, for every user: 
 • computes the average ratings given by the user for different movie genres , and • returns the top 5 genres for each user. The output must be sorted first by user and then by the average rating. 
 
You can download the input datasets from blackboard. There are two input files that you need to use: 
 
• Movies.dat. This file contains movies information and has the following format (genres are delimited by “|”) : 
 
 
Movie_id#movie_title#genre1|genre2|genre3|… 
 
 
• Ratings.dat. This file contains ratings given by users to different movies and have the following format: 
 
UserID#MovieID#Rating#Timestamp 
 
 
