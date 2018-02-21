import psycopg2
import sys, os, configparser
from pyspark import SparkConf, SparkContext

log_path = "/home/hadoop/logs/" # don't change this
aws_region = "us-east-1"  # don't change this
s3_bucket = "cs327e-fall2017-final-project" # don't change this
ratings_file = "s3a://" + s3_bucket + "/movielens/ratings.csv" # dataset for milestone 1
links_file = "s3a://" + s3_bucket + "/movielens/links.csv" # dataset for milestone 1

# global variable sc = Spark Context
sc = SparkContext()

# global variables for RDS connection
rds_config = configparser.ConfigParser()
rds_config.read(os.path.expanduser("~/config"))
rds_database = rds_config.get("default", "database") 
rds_user = rds_config.get("default", "user")
rds_password = rds_config.get("default", "password")
rds_host = rds_config.get("default", "host")
rds_port = rds_config.get("default", "port")

def init():
    # set AWS access key and secret account key
    cred_config = configparser.ConfigParser()
    cred_config.read(os.path.expanduser("~/.aws/credentials"))
    access_id = cred_config.get("default", "aws_access_key_id") 
    access_key = cred_config.get("default", "aws_secret_access_key") 
    
    # spark and hadoop configuration
    sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf.set("fs.s3a.access.key", access_id)
    hadoop_conf.set("fs.s3a.secret.key", access_key)
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"

################## general utility function ##################################

# Take the given RDD and print out a logfile of 30 rdd.collect() data
def print_rdd(rdd, logfile): 
  f = open(log_path + logfile, "w") 
  results = rdd.collect() 
  counter = 0
  for result in results:
    counter = counter + 1
    f.write(str(result) + "\n")
    if counter > 30:
      break
  f.close()

################## ratings file ##################################
  
  
#split the input line where "," exists and output movie_id and rating
def parse_line(line):
  fields = line.split(",")
  movie_id = int(fields[1])
  rating = fields[2]
  return (movie_id, rating)
 
 
#take the rating_files and parse through the textfile to create the rdd mapping and print the results to a textfile called "movie_rating_pairs"
init() 
lines = sc.textFile(ratings_file)
rdd = lines.map(parse_line) # movie_id, rating
print_rdd(rdd, "movie_rating_pairs")


#take the rdd that has been mapped from the source textFile and map the result by altering the rating value and mapping it in the form of (rating, 1)
rdd_pair = rdd.mapValues(lambda rating: (rating, 1)) # movie_id, (rating, 1)
print_rdd(rdd_pair, "movie_rating_one_pairs") # print rdd


#round the total sum of ratings and count the rating occurences and return both
def add_ratings_by_movie(rating1, rating2):
  # rating1 = (rating, occurrence)
  # rating2 = (rating, occurrence)
  rating_sum_total = round(float(rating1[0]) + float(rating2[0]), 2)
  rating_occurrences = rating1[1] + rating2[1]
  return (rating_sum_total, rating_occurrences)

#find the total of the rating sum and count the amount of occurrences and map it by grouping total rating and total occurances together then output a textfile.
rdd_totals = rdd_pair.reduceByKey(add_ratings_by_movie) # movie_id (total rating, total occurrences)
print_rdd(rdd_totals, "movie_total_rating_occurrences") # print rdd


# get average of movie ratings and round to 2 decimals  
def avg_ratings_by_movie(rating_total_occur):
  rating_total = float(rating_total_occur[0])
  rating_occur = rating_total_occur[1]
  avg_rating = round((rating_total / rating_occur), 2)
  return avg_rating


#get the average rating of the movie and the movie_id and map the values together and output the result of the average as a file named 'movie_rating_averages' then cache it
rdd_avgs = rdd_totals.mapValues(avg_ratings_by_movie) # movie_id, average rating
#print_rdd(rdd_avgs, "movie_rating_averages") # print rdd
rdd_avgs.cache()

################## links file ##################################


#parse the current link file string and split into movie_id and imdb_id then return the two variables
def parse_links_line(line):
  fields = line.split(",")
  movie_id = int(fields[0])
  imdb_id = int(fields[1])
  return (movie_id, imdb_id)
  
# lookup imdb id
links_lines = sc.textFile(links_file)
rdd_links = links_lines.map(parse_links_line) # movie_id, imdb_id
print_rdd(rdd_links, "rdd_links")

#take the results from the rdd_avgs and join the mapping with the rdd_links then output file as 'movielens_imdb_joined'
rdd_joined = rdd_avgs.join(rdd_links)
print_rdd(rdd_joined, "movielens_imdb_joined")


#Get the imdb_id and add the prefix to the string depending on the amount of digits to look something like tt0000001 then return imdb_id_str and avg
def add_imdb_id_prefix(tupl):
  movielens_id, atupl = tupl
  avg_rating, imdb_id = atupl
  imdb_id_str = str(imdb_id)
  
  if len(imdb_id_str) == 1:
     imdb_id_str = "tt000000" + imdb_id_str
  elif len(imdb_id_str) == 2:
     imdb_id_str = "tt00000" + imdb_id_str
  elif len(imdb_id_str) == 3:
     imdb_id_str = "tt0000" + imdb_id_str
  elif len(imdb_id_str) == 4:
     imdb_id_str = "tt000" + imdb_id_str
  elif len(imdb_id_str) == 5:
     imdb_id_str = "tt00" + imdb_id_str
  elif len(imdb_id_str) == 6:
     imdb_id_str = "tt0" + imdb_id_str
  else:
     imdb_id_str = "tt" + imdb_id_str
     
  return (imdb_id_str, avg_rating)

# add imdb_id prefix () 
rdd_ratings_by_imdb = rdd_joined.map(add_imdb_id_prefix) 
print_rdd(rdd_ratings_by_imdb, "rdd_ratings_by_imdb")


#save the ratings to the database
def save_rating_to_db(list_of_tuples):
  conn = psycopg2.connect(database=rds_database, user=rds_user, password=rds_password, host=rds_host, port=rds_port)
  conn.autocommit = True
  cur = conn.cursor()
  
  #change the input to tuples if not tuples
  for tupl in list_of_tuples:
    imdb_id_str, avg_rating = tupl
    
    #print "imdb_id_str = " + imdb_id_str
    #print "avg_rating = " + str(avg_rating)
    #update_stmt = "update title_ratings set movielens_rating = " + str(avg_rating) + " where title_id = '" + imdb_id_str + "'" 
    #print "update_stmt = " + update_stmt + "\n"
    update_stmt = "update title_ratings set movielens_rating = %s where title_id = %s" 

	#try saving saving the rating to database and throw a error message if cannot be saved
    try:
        cur.execute(update_stmt, (avg_rating, imdb_id_str))
    except Exception as e:
        print "Error in save_rating_to_db: ", e.message
  
#save the rdd_ratings for every partition to the database if possible using the save_rating_to_db function.
rdd_ratings_by_imdb.foreachPartition(save_rating_to_db)

# free up resources
sc.stop()