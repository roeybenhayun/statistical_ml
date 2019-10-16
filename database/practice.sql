/* how to creat a table with one entry */
create table movie (
     ID int,
     Genere varchar(255),
     TitleName varchar(255),
     Rating int,
     primary key (ID)
);

/* insert value to the table*/
insert into movie(ID, Genere, TitleName, Rating)
values (1,'kids', 'down', 5)

insert into movie(ID, Genere, TitleName, Rating)
values (2,'kids', 'down_up', 5)


insert into movie(ID, Genere, TitleName, Rating)
values (3,'comedy', 'up', 5)


insert into movie(ID, Genere, TitleName, Rating)
values (4,'action', 'unbroaken', 2)


insert into movie(ID, Genere, TitleName, Rating)
values (5,'action', 'in the river', 4)

/* select all from the table */
select * from movie

/* get value from the table according to some rule*/
select * from movie
where rating > 3

/* delete a table*/
/*drop table movie*/