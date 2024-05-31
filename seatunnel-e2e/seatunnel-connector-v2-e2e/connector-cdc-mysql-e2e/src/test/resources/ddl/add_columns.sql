CREATE DATABASE IF NOT EXISTS `shop`;
use shop;
INSERT INTO products
VALUES (default,"scooter","Small 2-wheel scooter",3.14),
       (default,"car battery","12V car battery",8.1),
       (default,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8),
       (default,"hammer","12oz carpenter's hammer",0.75),
       (default,"hammer","14oz carpenter's hammer",0.875),
       (default,"hammer","16oz carpenter's hammer",1.0),
       (default,"rocks","box of assorted rocks",5.3),
       (default,"jacket","water resistent black wind breaker",0.1),
       (default,"spare tire","24 inch spare tire",22.2);
update products set name = 'dailai' where id = 101;
delete from products where id = 102;

alter table products ADD COLUMN add_column1 varchar(64) not null default 'yy',ADD COLUMN add_column2 int not null default 1;

update products set name = 'dailai' where id = 110;
insert into products
values (default,"scooter","Small 2-wheel scooter",3.14,'xx',1),
       (default,"car battery","12V car battery",8.1,'xx',2),
       (default,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8,'xx',3),
       (default,"hammer","12oz carpenter's hammer",0.75,'xx',4),
       (default,"hammer","14oz carpenter's hammer",0.875,'xx',5),
       (default,"hammer","16oz carpenter's hammer",1.0,'xx',6),
       (default,"rocks","box of assorted rocks",5.3,'xx',7),
       (default,"jacket","water resistent black wind breaker",0.1,'xx',8),
       (default,"spare tire","24 inch spare tire",22.2,'xx',9);
delete from products where id = 118;

alter table products ADD COLUMN add_column3 float not null default 1.1;
alter table products ADD COLUMN add_column4 timestamp not null default current_timestamp();

delete from products where id = 113;
insert into products
values (default,"scooter","Small 2-wheel scooter",3.14,'xx',1,1.1,'2023-02-02 09:09:09'),
       (default,"car battery","12V car battery",8.1,'xx',2,1.2,'2023-02-02 09:09:09'),
       (default,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8,'xx',3,1.3,'2023-02-02 09:09:09'),
       (default,"hammer","12oz carpenter's hammer",0.75,'xx',4,1.4,'2023-02-02 09:09:09'),
       (default,"hammer","14oz carpenter's hammer",0.875,'xx',5,1.5,'2023-02-02 09:09:09'),
       (default,"hammer","16oz carpenter's hammer",1.0,'xx',6,1.6,'2023-02-02 09:09:09'),
       (default,"rocks","box of assorted rocks",5.3,'xx',7,1.7,'2023-02-02 09:09:09'),
       (default,"jacket","water resistent black wind breaker",0.1,'xx',8,1.8,'2023-02-02 09:09:09'),
       (default,"spare tire","24 inch spare tire",22.2,'xx',9,1.9,'2023-02-02 09:09:09');
update products set name = 'test' where id = 121;

