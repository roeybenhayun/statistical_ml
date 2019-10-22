
create table users (
     userid int,
     name text,
     primary key (userid)
);
create table movies (
     movieid int,
     title text,
     primary key (movieid)
);

create table taginfo (
     tagid int,
     content text,
     primary key (tagid)
);

create table genres (
     genreid int,
     name text,
     primary key (genreid)
);

create table ratings (
     userid int,
     movieid int,
     rating numeric check (rating >= 0.0 and rating <= 5.0),
     timestamp bigint,
     foreign key(userid) references users,
     foreign key(movieid) references movies,
     primary key(userid,movieid)
);

create table tags (
     userid int,
     movieid int,
     tagid int,
     timestamp bigint,
     foreign key (userid) references users,
     foreign key (movieid) references movies,
     foreign key (tagid) references taginfo
);

create table hasagenre (
     movieid int,
     genreid int,
     foreign key (movieid) references movies,
     foreign key (genreid) references genres 
);

/*
drop table hasagenre;
drop table tags;
drop table ratings;
drop table genres;
drop table taginfo;
drop table movies;
drop table users;
*/

/*
copy users from '/Users/roeybenhayun/Projects/ASU/Coursera-ASU-Database/course1/assignment1/exampleinput/users.dat' delimiter '%';
copy movies from '/Users/roeybenhayun/Projects/ASU/Coursera-ASU-Database/course1/assignment1/exampleinput/movies.dat' delimiter '%';
copy taginfo from '/Users/roeybenhayun/Projects/ASU/Coursera-ASU-Database/course1/assignment1/exampleinput/taginfo.dat' delimiter '%';
copy genres from '/Users/roeybenhayun/Projects/ASU/Coursera-ASU-Database/course1/assignment1/exampleinput/genres.dat' delimiter '%';
copy ratings from '/Users/roeybenhayun/Projects/ASU/Coursera-ASU-Database/course1/assignment1/exampleinput/ratings.dat' delimiter '%';
copy tags from '/Users/roeybenhayun/Projects/ASU/Coursera-ASU-Database/course1/assignment1/exampleinput/tags.dat' delimiter '%';
copy hasagenre from '/Users/roeybenhayun/Projects/ASU/Coursera-ASU-Database/course1/assignment1/exampleinput/hasagenre.dat' delimiter '%';
*/

/*get the max ID from the table. We know that it is the primary key*/


select genreid
from genres

select count(*) as bla
from genres


select genres.name as mname
from genres
where genres.genreid=18

select genres.name as name,avg(ratings.rating) as rating
from hasagenre,genres,ratings
where hasagenre.genreid=1 and genres.genreid=hasagenre.genreid and hasagenre.movieid=ratings.movieid
group by genres.name


/* query 1*/
DO $$
declare 
 a int;
 b text;
 c int;
BEGIN
   select MAX(genreid) from genres into c;
   RAISE NOTICE 'Number of geners = %', c;
   FOR counter IN 1..c LOOP
   select count(hasagenre.movieid) as moviecount,genres.name as name into a,b
   from hasagenre,genres
   where (hasagenre.genreid=counter) and (genres.genreid=hasagenre.genreid)
   group by genres.name;
   RAISE NOTICE '%,%,counter %', a,b,counter;
   END LOOP;
END; $$



/* query 2*/
DO $$
declare 
 a numeric;
 b text;
 c int;
BEGIN
   select MAX(genreid) from genres into c;
   RAISE NOTICE 'Number of geners = %', c;
   FOR counter IN 1..c LOOP
   select genres.name as name, avg(ratings.rating) as rating into b,a
   from hasagenre,genres,ratings
   where (hasagenre.genreid=counter) and (genres.genreid=hasagenre.genreid) and (hasagenre.movieid=ratings.movieid)
   group by genres.name;
   RAISE NOTICE '%,%,counter %', a,b,counter;
   END LOOP;
END; $$


/* query 3*/
select movies.title as name, count(ratings.rating) as rating
from ratings,movies
where (ratings.movieid = movies.movieid)
group by movies.title
having count(ratings.rating) > 5

/* query 4*/
select movies.movieid as movieid, movies.title as title
from genres,hasagenre,movies
where genres.name='Comedy' and hasagenre.genreid = genres.genreid and hasagenre.movieid=movies.movieid

/* query 5*/
select movies.title as text, avg(ratings.rating) as rating
from movies,ratings
where movies.movieid=ratings.movieid
group by movies.title

/* query 6*/
select avg(ratings.rating) as rating, count(ratings.rating) as count
from movies,ratings,genres
where genres.name='Comedy'  and genres.genreid=ratings.movieid 

/* query 7*/
select avg(ratings.rating) as rating, count(ratings.rating) as count
from movies,ratings,genres
where (genres.name='Comedy'or genres.name='Romance') and genres.genreid=ratings.movieid 
