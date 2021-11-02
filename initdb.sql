-- init db
create database if not exists userorders;
use userorders;
drop table if exists orders;
drop table if exists user;

create table user(
    id integer not null auto_increment,
    name varchar(60) not null,
    age integer not null,
    address varchar(60) not null,
    created_at datetime not null default CURRENT_TIMESTAMP,
    updated_at datetime,
    primary key (id)
);

create table orders(
    id integer not null auto_increment,
    id_user integer not null,
    spent integer not null,
    created_at datetime not null default CURRENT_TIMESTAMP,
    primary key(id),
    foreign key (id_user) references user(id)
);

-- populate db
insert into user(name, age, address) values
('francisco', 34, 'chalk avenue 23'),
('samuel', 86, 'crown road 101'),
('john', 20, 'salvador square 10');

-- populate orders
insert into orders(id_user, spent) values
(1, 30),
(1, 20),
(3, 100)
