create materialized view v_romance_cast as
select primary_name as cast_name, start_year as year, count(*) as movie_count
from title_basics tb
full outer join principals p on tb.title_id = p.title_id
join title_genres tg on tb.title_id = tg.title_id
join person_basics pb on pb.person_id = p.person_id
join person_professions pp on pb.person_id = pp.person_id
where start_year between 2012 and 2017 and title_type = 'movie' and genre = 'Romance' and (profession = 'actor' or profession = 'actress')
group by primary_name, start_year
having count(*) >= 3
order by start_year, count(*), primary_name;

create materialized view v_writer_director as
select primary_name as writer_name, count(*) as movie_count
from title_basics tb
full outer join writers w on tb.title_id = w.title_id
join person_basics pb on pb.person_id = w.person_id
join person_professions pp on pb.person_id = pp.person_id
full outer join title_ratings tr on tb.title_id = tr.title_id
where start_year = 2017 and title_type = 'movie' and profession = 'director' and average_rating >= 5.0
group by primary_name
having count(*) >= 2
order by count(*), primary_name;

create materialized view v_comedy_star as
select primary_name as star_name, start_year as year, count(*) as title_count
from title_basics tb
join stars s on tb.title_id = s.title_id
join person_basics pb on pb.person_id = s.person_id
join title_genres tg on tg.title_id = tb.title_id
where start_year >= 2015 and genre = 'Comedy'
group by primary_name, start_year
having count(*) >= 5
order by start_year, count(*);

create materialized view v_rating_title as
select distinct average_rating as rating, start_year as year, count(*) as title_count
from title_basics tb
full outer join title_episodes te on tb.title_id = te.parent_title_id
full outer join title_ratings tr on tb.title_id = tr.title_id
where start_year >= 2000 and episode_num >= 10
group by average_rating, start_year
having count(*) >= 10
order by start_year, average_rating;

create materialized view v_director_movie as
select primary_name as director_name, count(*) as title_count
from title_basics tb
join directors d on tb.title_id = d.title_id
join person_basics pb on pb.person_id = d.person_id
where start_year >= 2000 and title_type = 'movie'
group by primary_name
having count(*) > 50
order by count(*), primary_name;

CREATE MATERIALIZED VIEW v_high_rates AS
select genre,count(*)
from title_basics tb join title_ratings tr on tb.title_id=tr.title_id
join title_genres tg on tg.title_id=tb.title_id
where tr.average_rating>8 and title_type='movie'
group by genre 
order by count(*) desc;