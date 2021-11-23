
create database if not exists usersorders;

create table users(
    id integer not null auto_increment,
    name varchar(60) not null,
    age integer not null,
    address varchar(60) not null,
    created_at datetime not null default CURRENT_TIMESTAMP,
    updated_at datetime default null,
    primary key (id)
);


create table orders(
    id integer not null auto_increment,
    id_user integer not null,
    spent integer not null,
    status varchar(50) not null default 'processing',
    created_at datetime not null default CURRENT_TIMESTAMP,
    updated_at datetime default null,
    primary key(id),
    foreign key (id_user) references users(id)
);

-- populate db
insert into users(name, age, address) values
('francisco', 34, 'chalk avenue 23'),
('samuel', 86, 'crown road 101'),
('john', 20, 'salvador square 10');

-- populate orders
insert into orders(id_user, spent) values
(1, 30),
(1, 20),
(3, 100)
