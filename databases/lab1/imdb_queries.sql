/* Query 1: Find all comedy movies that are ordered by rating */
select tr.average_rating, tg.genre, tb.primary_title,tb.title_type
from title_basics tb join title_genres tg on tb.title_id = tg.title_id
join title_ratings tr on tr.title_id = tb.title_id
where tg.genre = 'Comedy'
and tb.title_type = 'movie'
and tr.average_rating is not null
order by tr.average_rating desc;

/* Query 2: people who are both director and writer */
select pb.primary_name
from person_basics pb join writers w on pb.person_id=w.person_id
join directors d on d.person_id=pb.person_id
where w.title_id=d.title_id;

/* Query 3: find all drama movies that are longer than 100 minutes */
select tb.primary_title, tb.runtime_minutes
from title_basics tb join title_genres tg on tb.title_id=tg.title_id
where tg.genre='Drama'
and tb.runtime_minutes>100
and tb.title_type='movie'
order by tb.runtime_minutes desc;  

/* Query 4: all the professions that Bob Carroll has */
select pp.profession
from person_professions pp join person_basics pb on pp.person_id=pb.person_id
where pb.primary_name='Bob Carroll';

/* Query 5: how many stars are listed */
select count(distinct person_id) from stars;

/* Query 6: people who lived longer than 80 years ordered by names */
select primary_name,  death_year, birth_year
from person_basics
where death_year is not null and birth_year is not null and death_year-birth_year>80
order by primary_name;

/* Query 7: what genre could a movie called "flashman" be (can be movies with same name) */
select tg.genre, tg.title_id
from title_basics tb join title_genres tg on tb.title_id=tg.title_id
where tb.primary_title='Flashman';

/* Query 8: find movies that got more than 1m votes */
select tb.primary_title, tr.num_votes
from title_basics tb join title_ratings tr on tb.title_id=tr.title_id
where tr.num_votes>=1000000
order by tr.num_votes desc;

/* Query 9: show the relationship between number of episodes and average rating a movie get ordered by episode numbers */
select tb.primary_title, te.episode_num, tr.average_rating
from title_basics tb join title_ratings tr on tb.title_id=tr.title_id
join title_episodes te on tb.title_id=te.title_id
where te.episode_num is not null and tr.average_rating is not null
order by te.episode_num;

/* Query 10: find all the composers ordered by the name */
select pb.primary_name,pp.profession 
from person_basics pb join person_professions pp on pb.person_id=pp.person_id
where pp.profession='composer'
order by pb.primary_name;

/* Query 11: Find tv series that starts in 2017 and have a rating of more than 9 sorted by average rating, from highest to lowest, then by name, alphabetically */

select distinct tb.primary_title, tr.average_rating
from title_basics tb
join title_episodes te on tb.title_id = te.parent_title_id
join title_ratings tr on tr.title_id = te.parent_title_id
where tb.title_type = 'tvSeries' and tb.start_year = 2017 and te.season_num is not null and tr.average_rating > 9
order by tr.average_rating desc, tb.primary_title;

/* Query 12: Find who died in 2017 and is under the age of 25 sorted by age, from youngest to oldest, then by name, alphabetically */

select distinct primary_name, birth_year, death_year, (death_year - birth_year) as Age
from person_basics
where birth_year is not null and death_year is not null and death_year = 2017 and (death_year - birth_year) < 25
order by (death_year - birth_year), primary_name;

/* Query 13: Find all movies that starts in 2017 that is over 3 hours long sorted by runtime_minutes, from shortest to longest, then by title, alphabetically */

select primary_title, runtime_minutes
from title_basics
where title_type = 'movie' and runtime_minutes > 180 and start_year = 2017
order by runtime_minutes, primary_title;

/* Query 14: Find all romance movies with Mirei Kiritani sorted by year, from oldest to newest */

select tb.start_year, tb.primary_title
from title_basics tb
join principals p on tb.title_id = p.title_id
join person_basics pb on pb.person_id = p.person_id
join title_genres tg on tb.title_id = tg.title_id
where pb.primary_name = 'Mirei Kiritani' and tb.title_type = 'movie' and tb.start_year is not null and tg.genre = 'Romance'
order by tb.start_year;

/* Query 15: Find all video games with Miyu Irino as a voice actor sorted by name, alphabetically */

select distinct tb.primary_title
from title_basics tb
join principals p on p.title_id = tb.title_id
join person_basics pb on pb.person_id = p.person_id
join person_professions pp on pb.person_id = pp.person_id
where pb.primary_name = 'Miyu Irino' and tb.title_type = 'videoGame' and pp.profession = 'actor'
order by tb.primary_title;

/* Query 16: Find all actors and actresses in the movie Yokokuhan sorted by name, alphabetically */

select * from
(select pb.primary_name, pp.profession
from person_basics pb
join principals p on pb.person_id = p.person_id
join title_basics tb on tb.title_id = p.title_id
join person_professions pp on pp.person_id = pb.person_id
where tb.original_title = 'Yokokuhan' and (pp.profession = 'actor' or pp.profession = 'actress'))
union
(select pb.primary_name, pp.profession
from person_basics pb
join stars s on pb.person_id = s.person_id
join title_basics tb on tb.title_id = s.title_id
join person_professions pp on pp.person_id = pb.person_id
where tb.original_title = 'Yokokuhan' and (pp.profession = 'actor' or pp.profession = 'actress'))
order by primary_name;

/* Query 17: Find all movies where Hayao Miyazaki is the director sorted by year, from oldest to newest, then by name, alphabetically */

select tb.primary_title, tb.start_year
from title_basics tb
join directors d on tb.title_id = d.title_id
join person_basics pb on pb.person_id = d.person_id
where pb.primary_name = 'Hayao Miyazaki' and tb.title_type = 'movie'
order by tb.start_year, tb.primary_title;

/* Query 18: Find all writers for the American Horror Story series sorted by name, alphabetically */

select pb.primary_name
from person_basics pb
join writers w on w.person_id = pb.person_id
join title_basics tb on tb.title_id = w.title_id
where tb.primary_title = 'American Horror Story'
order by pb.primary_name;

/* Query 19: Count the number of people who are born on or after 2000, and is both an actor/actress and a director */

select count(*) from
(select distinct pb.primary_name, pb.birth_year
from person_basics pb
join directors d on d.person_id = pb.person_id
join title_basics tb on tb.title_id = d.title_id
join stars s on s.person_id = d.person_id
where pb.birth_year is not null and pb.birth_year >= 2000);

/* Query 20: Find all genres associated with Hayao Miyazaki as a director sorted alphabetically */

select distinct tg.genre
from title_basics tb
join title_genres tg on tg.title_id = tb.title_id
join directors d on d.title_id = tb.title_id
join person_basics pb on pb.person_id = d.person_id
where pb.primary_name = 'Hayao Miyazaki'
order by tg.genre;