select count(*) from Person_Basics;
select count(*) from Title_Basics;
select count(*) from Directors;
select count(*) from Person_Professions;
select count(*) from Title_Episodes;
select count(*) from Title_Genres;
select count(*) from Title_Ratings;
select count(*) from Writers;
select count(*) from Principals;
select count(*) from Stars;
select * from Person_Basics limit 30;
select * from Title_Basics limit 30;
select * from Directors limit 30;
select * from Person_Professions limit 30;
select * from Title_Episodes limit 30;
select * from Title_Genres limit 30;
select * from Title_Ratings limit 30;
select * from Writers limit 30;
select * from Principals limit 30;
select * from Stars limit 30;
select distinct tg.genre
from title_basics tb
join title_genres tg on tg.title_id = tb.title_id
join directors d on d.title_id = tb.title_id
join person_basics pb on pb.person_id = d.person_id
where pb.primary_name = 'Hayao Miyazaki'
order by tg.genre;
select pb.primary_name
from person_basics pb
join writers w on w.person_id = pb.person_id
join title_basics tb on tb.title_id = w.title_id
where tb.primary_title = 'American Horror Story'
order by pb.primary_name;
select distinct tb.primary_title, tr.average_rating
from title_basics tb
join title_episodes te on tb.title_id = te.parent_title_id
join title_ratings tr on tr.title_id = te.parent_title_id
where tb.title_type = 'tvSeries' and tb.start_year = 2017 and te.season_num is not null and tr.average_rating > 9
order by tr.average_rating desc, tb.primary_title;
select count(*) from
(select distinct pb.primary_name, pb.birth_year
from person_basics pb
join directors d on d.person_id = pb.person_id
join title_basics tb on tb.title_id = d.title_id
join stars s on s.person_id = d.person_id
where pb.birth_year is not null and pb.birth_year >= 2000) as foo;
select distinct tb.primary_title
from title_basics tb
join principals p on p.title_id = tb.title_id
join person_basics pb on pb.person_id = p.person_id
join person_professions pp on pb.person_id = pp.person_id
where pb.primary_name = 'Miyu Irino' and tb.title_type = 'videoGame' and pp.profession = 'actor'
order by tb.primary_title;