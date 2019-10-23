/*
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
*/
/*
drop table hasagenre;
drop table tags;
drop table ratings;
drop table genres;
drop table taginfo;
drop table movies;
drop table users;
drop table query1;
drop table query2;
drop table query3;
drop table query4;
drop table query5;
drop table query6;
drop table query7;
drop table query8;
drop table query9;
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


/* query 1*/
create table query1 as( 
     select genres.name as name, count(movies.movieid) as moviecount
     from movies,genres,hasagenre
     where genres.genreid=hasagenre.genreid and hasagenre.movieid=movies.movieid
     group by genres.name
);


/* query 2*/
create table query2 as( 
     select genres.name as name, avg(ratings.rating) as rating
     from genres,ratings,movies,hasagenre
     where genres.genreid=hasagenre.genreid and hasagenre.movieid=movies.movieid and ratings.movieid=movies.movieid
     group by genres.name
);

/* query 3*/
create table query3 as( 
     select movies.title as title, count(ratings.rating) as CountOfRatings
     from ratings,movies
     where (ratings.movieid = movies.movieid)
     group by movies.title
     having count(ratings.rating) >= 10
);

/* query 4*/
create table query4 as(
     select movies.movieid as movieid, movies.title as title
     from genres,hasagenre,movies
     where genres.name='Comedy' and hasagenre.genreid = genres.genreid and hasagenre.movieid=movies.movieid
);


/* query 5*/
create table query5 as(
     select movies.title as title, avg(ratings.rating) as average
     from movies,ratings
     where movies.movieid=ratings.movieid
     group by movies.title
);

/* query 6*/
create table query6 as(
     select avg(ratings.rating) as average, count(ratings.rating) as count
     from movies,ratings,genres,hasagenre
     where genres.name='Comedy'  and genres.genreid=hasagenre.genreid and ratings.movieid = movies.movieid
);

/* query 7*/
create table query7 as(
     select avg(ratings.rating) as average
     from movies,ratings,genres
     where (genres.name='Comedy'or genres.name='Romance') and ratings.movieid=movies.movieid
);

/* query 8*/
create table query8 as(
     select avg(ratings.rating) as average
     from movies,ratings,genres
     where (genres.name='Comedy'or genres.name!='Romance') and ratings.movieid=movies.movieid
);

/* query 9*/
create table query9 as(
     select movies.movieid as movieid, ratings.rating
     from ratings,movies
     where ratings.userid=:v1 and movies.movieid=ratings.movieid
);