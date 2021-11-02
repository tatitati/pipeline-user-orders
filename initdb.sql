create database userorders;

use userorders;

drop table if exists user;
drop table if exists orders;

create table user(
    id integer not null auto_increment,
    name varchar(60) not null,
    age integer not null,
    address varchar(60) not null,
    created_at datetime not null,
    updated_at datetime,
    primary key (id)
);

create table orders(
    id integer not null auto_increment,
    id_user integer not null,
    spent integer not null,
    created_at datetime not null,
    primary key(id),
    foreign key (id_user) references user(id)
);