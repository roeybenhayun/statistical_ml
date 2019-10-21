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
/*like string regex when using not like or like keyword. Take a look 
on the pattern. Another keyword is the desc which is descending, the order*/
/*
select *
from nobel_win
where subject not like  'P%' order by year desc ,winner
*/

/*
between keyword - find in range
select pro_name
from item_mast
where pro_price between 200 and 600
*/
/*
math operation on a column - in this case average
SELECT AVG(pro_price) FROM item_mast 
  WHERE pro_com=16;
*/
/*
how to order items
SELECT pro_name, pro_price
 FROM item_mast 
where pro_price>=250 order by pro_price desc, pro_name 
*/

/*
nested select
select pro_name, pro_price
from item_mast
where pro_price=(select min(pro_price) from item_mast)
*/

/*
Write a SQL statement to display all those orders 
by the customers not located in the same cities where their salesmen live.

SELECT orders.ord_no, customer.cust_name, orders.customer_id, orders.salesman_id
FROM salesman, customer, orders
WHERE customer.city != salesman.city
AND orders.customer_id = customer.customer_id
AND orders.salesman_id = salesman.salesman_id;
*/

sselect salesman.name, salesman.commission, customer.cust_name, customer.city
from salesman, customer
where customer.salesman_id = salesman.salesman_id and salesman.commission between .12 and .14





DO $$
BEGIN
  RAISE NOTICE 'notice message';
END
$$;


DO $$ 
DECLARE
   start_at CONSTANT time := now();
BEGIN 
   RAISE NOTICE 'Start executing block at %', start_at;
END $$;


DO $$ 
DECLARE
   VAT CONSTANT NUMERIC := 0.1;
   net_price    NUMERIC := 20.5;
BEGIN 
   RAISE NOTICE 'The selling price is %', net_price * ( 1 + VAT );
END $$;

/*
DO $$ 
BEGIN 
RAISE NOTICE 'Create movie table'; 
END $$;
*/



/*
create or replace function getTableDataPath(tablePath varchar) returns varchar as $$
begin

     if (tablePath = "movies" ) then
          RAISE NOTICE 'table is not null';
          return "/Users/roeybenhayun/Projects/ASU/Coursera-ASU-Database/course1/assignment1/exampleinput";
     end if

          return "/Users/roeybenhayun/Projects/ASU/Coursera-ASU-Database/course1/assignment1/exampleinput";

end
$$
copy movie from public.getTableDataPath("movies") delimiter ',';
*/
