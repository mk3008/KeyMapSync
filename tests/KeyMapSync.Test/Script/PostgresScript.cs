namespace KeyMapSync.Test.Script;

public static class PostgresScript
{
    public static string InitializeSql => @"
drop table if exists integration_sale_detail
;
drop table if exists integration_sale_detail_ext_ec_shop_article
;
drop table if exists integration_sale_detail_ext_store_sale_detail
;
create table integration_sale_detail (
    integration_sale_detail_id serial8 primary key,
    sale_date date, 
    article_name text,
    unit_price int8, 
    quantity int8, 
    price int8
)
;
create table integration_sale_detail_ext_ec_shop_article (
    extension_id serial8 primary key,
    integration_sale_detail_id int8,
    ec_shop_article_id int8,
    unique(integration_sale_detail_id)
)
;
create table integration_sale_detail_ext_store_sale_detail (
    extension_id serial8 primary key,
    integration_sale_detail_id int8,
    store_article_id int8, 
    remarks text,
    unique(integration_sale_detail_id)
)
;
drop table if exists ec_shop_article
;
drop table if exists ec_shop_sale
;
drop table if exists ec_shop_sale_detail
;
create table ec_shop_article (
    ec_shop_article_id serial8 primary key,
    article_name text, 
    unit_price int8,
    create_timestamp timestamp
)
;
create table ec_shop_sale (
    ec_shop_sale_id serial8 primary key,
    sale_date date, 
    create_timestamp timestamp
)
;
create table ec_shop_sale_detail (
    ec_shop_sale_detail_id serial8 primary key,
    ec_shop_sale_id int8,
    ec_shop_article_id int8,
    unit_price int8, 
    quantity int8, 
    price int8
)
;
drop table if exists store_article
;
drop table if exists store_sale
;
drop table if exists store_sale_detail
;
create table store_article (
    store_article_id serial8 primary key,
    article_name text, 
    unit_price int8,
    create_timestamp timestamp
)
;
create table store_sale (
    store_sale_id serial8 primary key,
    sale_date date, 
    create_timestamp timestamp
)
;
create table store_sale_detail (
    store_sale_detail_id serial8 primary key,
    store_sale_id int8,
    store_article_id int8,
    unit_price int8, 
    quantity int8, 
    price int8, 
    remarks text
)
;
drop table if exists integration_sale_detail__map_ec_shop_sale_detail
;
drop table if exists integration_sale_detail__offset
;
drop table if exists integration_sale_detail__sync
;
drop table if exists integration_sale_detail__version
;
";
}