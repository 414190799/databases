select title_type, start_year as year, genre, count(*)::int as appalling_titles
from title_basics tb
join title_genres tg on tb.title_id = tg.title_id
join title_ratings tr on tb.title_id = tr.title_id
where average_rating <= 2
group by title_type, start_year, genre;

select title_type, start_year as year, genre, count(*)::int as average_titles
from title_basics tb
join title_genres tg on tb.title_id = tg.title_id
join title_ratings tr on tb.title_id = tr.title_id
where average_rating > 2 and average_rating < 8
group by title_type, start_year, genre;

select title_type, start_year as year, genre, count(*)::int as outstanding_titles
from title_basics tb
join title_genres tg on tb.title_id = tg.title_id
join title_ratings tr on tb.title_id = tr.title_id
where average_rating >= 8
group by title_type, start_year, genre;

create table Title_Rating_Facts_Appalling as
select title_type, start_year as year, genre, count(*)::int as appalling_titles
from title_basics tb
join title_genres tg on tb.title_id = tg.title_id
join title_ratings tr on tb.title_id = tr.title_id
where average_rating <= 2
group by title_type, start_year, genre;

create table Title_Rating_Facts_Average as
select title_type, start_year as year, genre, count(*)::int as average_titles
from title_basics tb
join title_genres tg on tb.title_id = tg.title_id
join title_ratings tr on tb.title_id = tr.title_id
where average_rating > 2 and average_rating < 8
group by title_type, start_year, genre;

create table Title_Rating_Facts_Outstanding as
select title_type, start_year as year, genre, count(*)::int as outstanding_titles
from title_basics tb
join title_genres tg on tb.title_id = tg.title_id
join title_ratings tr on tb.title_id = tr.title_id
where average_rating >= 8
group by title_type, start_year, genre;

select *
from title_rating_facts_appalling
natural full outer join title_rating_facts_average
natural full outer join title_rating_facts_outstanding;

create table Title_Rating_Facts as
select *
from title_rating_facts_appalling
natural full outer join title_rating_facts_average
natural full outer join title_rating_facts_outstanding;

update title_rating_facts
set appalling_titles = 0
where appalling_titles is null;

update title_rating_facts
set average_titles = 0
where average_titles is null;

update title_rating_facts
set outstanding_titles = 0
where outstanding_titles is null;

delete from title_rating_facts
where year is null;

alter table title_rating_facts add primary key (title_type, year, genre);

select year, genre, sum(outstanding_titles) as outstanding_count
from title_rating_facts
where year >= 1930
group by year, genre
having sum(outstanding_titles) > 0
order by year, genre
limit 100;

create view v_outstanding_titles_by_year_genre as
select year, genre, sum(outstanding_titles) as outstanding_count
from title_rating_facts
where year >= 1930
group by year, genre
having sum(outstanding_titles) > 0
order by year, genre
limit 100;