namespace KeyMapSync.Test.Script;

public static class Store
{
    public static string InitializeSql => @"
drop table if exists store_article
;
drop table if exists store_sale
;
drop table if exists store_sale_detail
;
create table store_article (
    store_article_id integer primary key autoincrement,
    article_name text, 
    unit_price integer,
    create_timestamp timestamp
)
;
create table store_sale (
    store_sale_id integer primary key autoincrement,
    sale_date date, 
    create_timestamp timestamp
)
;
create table store_sale_detail (
    store_sale_detail_id integer primary key autoincrement,
    store_sale_id integer,
    store_article_id integer,
    unit_price integer, 
    quantity integer, 
    price integer, 
    remarks text
)
;";

    public static string CreateDataSql => @"
insert into store_article (store_article_id, article_name, unit_price, create_timestamp)
values 
(10, 'apple' , 100, current_timestamp),
(20, 'orange', 150, current_timestamp),
(30, 'coffee', 200, current_timestamp),
(40, 'tea'   , 250, current_timestamp)
;
insert into store_sale (store_sale_id, sale_date, create_timestamp)
values 
(100, '2000/01/01', '2000/01/01'),
(200, '2000/01/02', '2000/01/02'),
(300, '2000/02/01', '2000/02/01'),
(400, '2000/02/02', '2000/02/02')
;
insert into store_sale_detail (store_sale_id, store_article_id, unit_price, quantity, price, remarks)
select
    v.column1 as store_sale_id,
    a.store_article_id,
    a.unit_price, 
    v.column3 as quantity,
    a.unit_price * v.column3 + v.column4 as price,
    v.column5 || ' ' || v.column4 as remakrs
from
    (
        values 
        (100, 10,  5, -100, 'disount'),
        (100, 20,  7,    0, ''),
        (200, 10,  1,    0, ''),
        (200, 30,  2,    0, ''),
        (200, 40,  3,    0, ''),
        (300, 20,  5,    0, ''),
        (300, 40,  4,    0, ''),
        (400, 10,  3,    0, ''),
        (400, 20,  7,    0, ''),
        (400, 30, 20,    0, ''),
        (400, 40,  1,    0, '')
    )v 
    inner join store_article a on v.column2 = a.store_article_id
;";
}



