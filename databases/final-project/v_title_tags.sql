
/* select and show all the titles with tags strings that have lengths greater than 10 that has aired since 2000.

create view v_title_tags as
  select tag, length(cast (tag as text)), tb.start_year
  from title_basics tb join title_tags tt on tb.title_id=tt.title_id
  where length(cast (tag as text))>10 and tb.start_year >= 2000
  group by tag, tb.start_year
  order by length(cast (tag as text))
;