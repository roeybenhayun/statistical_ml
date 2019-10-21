
create table users (
     userid int,
     name text,
     primary key (userid)
)

create table movies (
     movieid int,
     title text,
     primary key (movieid)
)

create table taginfo (
     tagid int,
     content text,
     primary key (tagid)
)

create table geners (
     generid int,
     name text,
     primary key (generid)
)

create table ratings (
     userid int,
     movieid int,
     rating numeric check (rating >= 0.0 and rating <= 5.0),
     timestamp bigint,
     foreign key(userid) references users,
     foreign key(movieid) references movies,
     primary key(userid,movieid)
)

create table tags (
     userid int,
     movieid int,
     tagid int,
     timestamp bigint,
     foreign key (userid) references users,
     foreign key (movieid) references movies,
     foreign key (tagid) references taginfo
)

create table hasgenre (
     movieid int,
     generid int,
     foreign key (movieid) references movies,
     foreign key (generid) references geners
)

copy users from '/Users/roeybenhayun/Projects/ASU/Coursera-ASU-Database/course1/assignment1/exampleinput/users.dat' delimiter '%'
copy movies from '/Users/roeybenhayun/Projects/ASU/Coursera-ASU-Database/course1/assignment1/exampleinput/movies.dat' delimiter '%'
copy taginfo from '/Users/roeybenhayun/Projects/ASU/Coursera-ASU-Database/course1/assignment1/exampleinput/taginfo.dat' delimiter '%'
copy geners from '/Users/roeybenhayun/Projects/ASU/Coursera-ASU-Database/course1/assignment1/exampleinput/genres.dat' delimiter '%'
copy ratings from '/Users/roeybenhayun/Projects/ASU/Coursera-ASU-Database/course1/assignment1/exampleinput/ratings.dat' delimiter '%'
copy tags from '/Users/roeybenhayun/Projects/ASU/Coursera-ASU-Database/course1/assignment1/exampleinput/tags.dat' delimiter '%'
copy hasgenre from '/Users/roeybenhayun/Projects/ASU/Coursera-ASU-Database/course1/assignment1/exampleinput/hasagenre.dat' delimiter '%'




/*select * from movie*/
/*drop table movie*/