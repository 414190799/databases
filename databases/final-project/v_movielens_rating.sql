
/* select number of high rating movies that both ratings systems agree on, group by movie type.

create view v_movielens_rating as
  select genre, count(*) 
  from title_ratings tr join title_genres tg on tr.title_id=tg.title_id
  where average_rating>7 and movielens_rating>4
  group by genre
  order by count(*);