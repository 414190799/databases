/* find the number of movies started in each year that have more than 1 million budget to start.
create view v_title_financials as
  select start_year, count(*)
  from title_basics tb join title_financials tf on tb.title_id= tf.title_id
  where budget>1000000
  group by start_year;






