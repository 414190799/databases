CREATE EXTERNAL TABLE IF NOT EXISTS imdb.Directors (
  `title_id` string,
  `person_id` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/directors/'
TBLPROPERTIES ('has_encrypted_data'='false');

CREATE EXTERNAL TABLE IF NOT EXISTS imdb.Person_Basics (
  `person_id` string,
  `primary_name` string,
  `birth_year` int,
  `death_year` int 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/person_basics/'
TBLPROPERTIES ('has_encrypted_data'='false');

CREATE EXTERNAL TABLE IF NOT EXISTS imdb.Person_Professions (
  `person_id` string,
  `profession` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/person_professions/'
TBLPROPERTIES ('has_encrypted_data'='false');

CREATE EXTERNAL TABLE IF NOT EXISTS imdb.Principals (
  `title_id` string,
  `person_id` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/principals/'
TBLPROPERTIES ('has_encrypted_data'='false');

CREATE EXTERNAL TABLE IF NOT EXISTS imdb.Stars (
  `person_id` string,
  `title_id` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/stars/'
TBLPROPERTIES ('has_encrypted_data'='false');

CREATE EXTERNAL TABLE IF NOT EXISTS imdb.Title_Basics (
  `title_id` string,
  `title_type` string,
  `primary_title` string,
  `original_title` string,
  `is_adult` boolean,
  `start_year` int,
  `end_year` int,
  `runtime_minutes` int 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/title_basics/'
TBLPROPERTIES ('has_encrypted_data'='false');

CREATE EXTERNAL TABLE IF NOT EXISTS imdb.Title_Episodes (
  `title_id` string,
  `parent_title_id` string,
  `season_num` int,
  `episode_num` int 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/title_episodes/'
TBLPROPERTIES ('has_encrypted_data'='false');

CREATE EXTERNAL TABLE IF NOT EXISTS imdb.Title_Genres (
  `title_id` string,
  `genre` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/title_genres/'
TBLPROPERTIES ('has_encrypted_data'='false');

CREATE EXTERNAL TABLE IF NOT EXISTS imdb.Title_Ratings (
  `title_id` string,
  `average_rating` double,
  `num_votes` int 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/title_ratings/'
TBLPROPERTIES ('has_encrypted_data'='false');

CREATE EXTERNAL TABLE IF NOT EXISTS imdb.Writers (
  `title_id` string,
  `person_id` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://cs327e-fall2017-imdb/athena/writers/'
TBLPROPERTIES ('has_encrypted_data'='false');