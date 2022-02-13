namespace KeyMapSync.HeaderTest.Script;

public static class EcShop
{
    public static string InitializeSql => @"
drop table if exists ec_shop_article
;
drop table if exists ec_shop_sale
;
drop table if exists ec_shop_sale_detail
;
create table ec_shop_article (
    ec_shop_article_id integer primary key autoincrement,
    article_name text, 
    unit_price integer,
    create_timestamp timestamp
)
;
create table ec_shop_sale (
    ec_shop_sale_id integer primary key autoincrement,
    shop_id integer,
    sale_date date, 
    create_timestamp timestamp
)
;
create table ec_shop_sale_detail (
    ec_shop_sale_detail_id integer primary key autoincrement,
    ec_shop_sale_id integer,
    ec_shop_article_id integer,
    unit_price integer, 
    quantity integer, 
    price integer
)
;";

    public static string CreateDataSql => @"
insert into ec_shop_article (ec_shop_article_id, article_name, unit_price, create_timestamp)
values 
(10, 'apple' , 100, current_timestamp),
(20, 'orange', 150, current_timestamp),
(30, 'coffee', 200, current_timestamp),
(40, 'tea'   , 250, current_timestamp)
;
insert into ec_shop_sale (ec_shop_sale_id, shop_id, sale_date, create_timestamp)
values 
(100, 1, '2000/01/01', '2000/01/01'),
(200, 1, '2000/01/02', '2000/01/02'),
(300, 2, '2000/02/01', '2000/02/01'),
(400, 2, '2000/02/02', '2000/02/02')
;
insert into ec_shop_sale_detail (ec_shop_sale_id, ec_shop_article_id, unit_price, quantity, price)
select
    v.column1 as ec_shop_sale_id,
    a.ec_shop_article_id,
    a.unit_price, 
    v.column3 as quantity,
    a.unit_price * v.column3 as price
from
    (
        values 
        (100, 10, 5),
        (100, 20, 7),
        (200, 10, 1),
        (200, 30, 2),
        (200, 40, 3),
        (300, 20, 5),
        (300, 40, 4),
        (400, 10, 3),
        (400, 20, 7),
        (400, 30, 20),
        (400, 40, 1)
    )v 
    inner join ec_shop_article a on v.column2 = a.ec_shop_article_id
;";

    public static string CreateExtendDataSql => @"
insert into ec_shop_article (ec_shop_article_id, article_name, unit_price, create_timestamp)
values 
(50, 'beef' , 500, current_timestamp)
;
insert into ec_shop_sale (ec_shop_sale_id, sale_date, create_timestamp)
values 
(800, '2000/03/10', '2000/03/10')
;
insert into ec_shop_sale_detail (ec_shop_sale_id, ec_shop_article_id, unit_price, quantity, price)
select
    v.column1 as ec_shop_sale_id,
    a.ec_shop_article_id,
    a.unit_price, 
    v.column3 as quantity,
    a.unit_price * v.column3 as price
from
    (
        values 
        (800, 50, 6)
    )v 
    inner join ec_shop_article a on v.column2 = a.ec_shop_article_id
;";
}



