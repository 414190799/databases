import psycopg2
import sys, os, configparser, csv
from pyspark import SparkConf, SparkContext

log_path = "/home/hadoop/logs/" # don't change this
aws_region = "us-east-1"  # don't change this
s3_bucket = "cs327e-fall2017-final-project" # don't change this
the_numbers_files = "s3a://" + s3_bucket + "/the-numbers/*" # dataset for milestone 3

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
  
################## process the-numbers dataset #################################

def parse_line(line):

    # add logic for parsing and cleaning the fields as specified in step 4 of assignment sheet
    line = line.split("\t")
    line[0] = line[0].strip()
    year_str = ""
    for i in range (len(line[0])-4, len(line[0]), 1):
    	year_str += line[0][i]
    	
    release_year = int(year_str)
    line[1] = line[1].strip()
    line[1] = line[1].encode('utf-8')
    movie_title = line[1].upper()
    line[2] = line[2].strip()
    if line[2] == "Thriller/Suspense" :
    	line[2] = "Thriller"
    if line[2] == "Black Comedy" :
    	line[2] = "Comedy"
    if line[2] == "Romantic Comedy" :
    	line[2] = "Romance"
    genre = line[2]
    line[3] = line[3].strip()
    newString = ""
    for char in line[3]:
    	if char == "$" or char == "," or char == "\"" :
    		continue
    	else:
    		newString += char
    if len(newString) == 0:
    	budget = -1
    else:
    	budget = int(newString)
    	
    newString1 = ""
    line[4] = line[4].strip()
    for char in line[4]:
    	if char == "$" or char == "," or char == "\"" :
    		continue
    	else:
    		newString1 += char
    if len(newString1) == 0:
    	box_office = -1
    else:
    	box_office = int(newString1)
    	
    return (release_year, movie_title, genre, budget, box_office)  
  
init() 
base_rdd = sc.textFile(the_numbers_files)
mapped_rdd = base_rdd.map(parse_line) 
print_rdd(mapped_rdd, "mapped_rdd")


def save_to_db(list_of_tuples):
  
    conn = psycopg2.connect(database=rds_database, user=rds_user, password=rds_password, host=rds_host, port=rds_port)
    conn.autocommit = True
    cur = conn.cursor()
    
    select_stmt = "SELECT title_id, count(*) FROM title_basics tb WHERE UPPER(primary_title) LIKE %s AND start_year = %s GROUP BY title_id"
    insert_stmt = "INSERT INTO title_financials(title_id, budget, box_office) VALUES(%s, %s, %s)"
    
    for tupl in list_of_tuples:
    	release_year, movie_title, genre, budget, box_office = tupl
    	record_count = 0
    	# add logic to look up the title_id in the database as specified in step 5 of assignment sheet
    	# add logic to write out the financial record to the database as specified in step 5 of assignment sheet
    	try:
    		cur.execute(select_stmt, (movie_title, release_year))
    		rows = cur.fetchall()
    		#print("\n")
    		#print(movie_title, release_year)
    	
    		for row in rows:
    			title_id = row[0]
    			record_count = row[1]
    			print("Title_id is : " + str(title_id) + " record count is: " + str(record_count) + "\n")
    		if record_count == 1:
    			cur.execute(insert_stmt, (title_id, budget, box_office))
    		elif record_count > 1:
    			if box_office > 0:
    				select_stmt = "SELECT tb.title_id, count(*) FROM title_basics tb WHERE UPPER(primary_title) LIKE %s AND start_year = %s AND tb.title_type != 'tvEpisode' GROUP BY title_id"
    			elif box_office < 1:
    				select_stmt = "SELECT tb.title_id, tg.genre, count(*) FROM title_basics tb JOIN title_genres tg on tb.title_id=tg.title_id WHERE upper(primary_title) LIKE %s AND start_year = %s AND tb.title_type != 'tvEpisode' AND tb.genre = %s GROUP BY title_id, genre"
    			cur.execute(insert_stmt, (title_id, budget, box_office))
    		
    	except Exception as e:
    		print "Error in running statement: " , e.message
    	
    
    cur.close()
    conn.close()
  
  
mapped_rdd.foreachPartition(save_to_db)

# free up resources
sc.stop() 