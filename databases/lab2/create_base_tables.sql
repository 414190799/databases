\c dev;
drop database if exists imdb;
create database imdb;
\c imdb;
create table Person_Basics(
   person_id varchar(10) primary key,
   primary_name varchar(110),
   birth_year int,
   death_year int
);
create table Title_Basics(
   title_id varchar(10) primary key,
   title_type varchar(20),
   primary_title varchar(330),
   original_title varchar(330),
   is_adult boolean,
   start_year int,
   end_year int,
   runtime_minutes int
);
create table Directors(
   title_id varchar(10),
   person_id varchar(10),
   primary key(title_id, person_id),
   foreign key(title_id) references Title_Basics(title_id),
   foreign key(person_id) references Person_Basics(person_id)
);
create table Person_Professions(
   person_id varchar(10),
   profession varchar(30),
   primary key(person_id, profession),
   foreign key(person_id) references Person_Basics(person_id)
);
create table Title_Episodes(
   title_id varchar(10) primary key,
   parent_title_id varchar(10),
   season_num int,
   episode_num int,
   foreign key(title_id) references Title_Basics(title_id),
   foreign key(parent_title_id) references Title_Basics(title_id)
);
create table Title_Genres(
   title_id varchar(10),
   genre varchar(20),
   primary key(title_id, genre),
   foreign key(title_id) references Title_Basics(title_id)
);
create table Title_Ratings(
   title_id varchar(10) primary key,
   average_rating numeric(3, 1),
   num_votes int,
   foreign key(title_id) references Title_Basics(title_id)
);
create table Writers(
   title_id varchar(10),
   person_id varchar(10),
   primary key(title_id, person_id),
   foreign key(title_id) references Title_Basics(title_id),
   foreign key(person_id) references Person_Basics(person_id)
);
create table Principals(
   title_id varchar(10),
   person_id varchar(10),
   primary key(title_id, person_id)
);
create table Stars(
   person_id varchar(10),
   title_id varchar(10),
   primary key(person_id, title_id)
);
   alter table Principals add foreign key(title_id) references Title_Basics(title_id);
   alter table Principals add foreign key(person_id) references Person_Basics(person_id);
   alter table Stars add foreign key(person_id) references Person_Basics(person_id);
   alter table Stars add foreign key(title_id) references Title_Basics(title_id);