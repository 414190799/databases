create table Songs(
  song_id varchar(10) primary key,
  song_title varchar(60),
  song_duration numeric(10,1)
);

create table Singer_Songs(
  person_id varchar(10),
  song_id varchar(10),
  primary key(person_id,song_id),
  foreign key(song_id) references Songs(song_id)
);

alter table person_basics add column gender varchar(1);

\copy Songs from /Users/danielchoi/Documents/CS327E-Databases/cinemalytics-with-headers/songs.csv (header TRUE, format csv, delimiter ',', null '', encoding 'UTF8');

\copy Singer_Songs from /Users/danielchoi/Documents/CS327E-Databases/cinemalytics-with-headers/singer_songs.csv (header TRUE, format csv, delimiter ',', null '', encoding 'UTF8');

create table bpersons(
  person_id varchar(10) primary key,
  primary_name varchar(110),
  gender varchar(1),
  dob date
);

\copy bpersons from /Users/danielchoi/Documents/CS327E-Databases/cinemalytics-with-headers/persons.csv (header TRUE, format csv, delimiter ',', null '', encoding 'UTF8');

create table btitle_songs(
  song_id varchar(10) primary key,
  movie_id varchar(10)
);

\copy btitle_songs from /Users/danielchoi/Documents/CS327E-Databases/cinemalytics-with-headers/title_songs.csv (header TRUE, format csv, delimiter ',', null '', encoding 'UTF8');

create table btitles(
  movie_id varchar(10) primary key,
  imdb_id varchar(10),
  primary_title varchar(110),
  original_title varchar(110),
  genre varchar(20),
  release_year int
);

\copy btitles from /Users/danielchoi/Documents/CS327E-Databases/cinemalytics-with-headers/titles.csv (header TRUE, format csv, delimiter ',', null '', encoding 'UTF8');
  
insert into person_basics(person_id,primary_name,gender,birth_year);
select distinct b.person_id, primary_name, gender, extract(year from dob) from bpersons b join singer_songs ss on ss.person_id=b.person_id;

create table Title_Songs(
  title_id varchar(10),
  song_id varchar(10),
  primary key(title_id,song_id),
  foreign key(title_id) references Title_Basics(title_id),
  foreign key(song_id) references Songs(song_id)
);


delete from singer_songs ss
where ss.person_id not in (select ss.person_id from singer_songs ss join person_basics pb on ss.person_id=pb.person_id);

insert into title_songs(title_id,song_id)
select imdb_id, song_id from btitles bt join btitle_songs bs on bt.movie_id=bs.movie_id
join title_basics tb on bt.imdb_id = tb.title_id;



drop table bpersons;
drop table btitle_songs;
drop table btitles;

alter table singer_songs add foreign key(person_id) references person_basics(person_id);



