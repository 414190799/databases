//how many songs each singer has
create view songs_created as
select pb.person_id, count(song_id)
from singer_songs ss join person_basics pb on ss.person_id=pb.person_id
group by pb.person_id 
order by count(song_id) desc;

//how many songs in each movie that is longer than 3mins
create view count_songs as
select ts.title_id, count(s.song_id)
from title_songs ts join songs s on s.song_id = ts.song_id
where song_duration>3
group by ts.title_id
order by count(s.song_id) desc;
