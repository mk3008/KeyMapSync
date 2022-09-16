namespace KeyMapSync.Test.Script;

public static class SqliteScript
{
    public static string InitializeSql => @"
drop table if exists integration_sale_detail
;
drop table if exists integration_sale_detail_ext_ec_shop_article
;
drop table if exists integration_sale_detail_ext_store_sale_detail
;
create table integration_sale_detail (
    integration_sale_detail_id integer primary key autoincrement,
    sale_date date, 
    article_name text,
    unit_price integer, 
    quantity integer, 
    price integer
)
;
create table integration_sale_detail_ext_ec_shop_article (
    extension_id integer primary key autoincrement,
    integration_sale_detail_id integer,
    ec_shop_article_id integer,
    unique(integration_sale_detail_id)
)
;
create table integration_sale_detail_ext_store_sale_detail (
    extension_id integer primary key autoincrement,
    integration_sale_detail_id integer,
    store_article_id integer, 
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
    ec_shop_article_id integer primary key autoincrement,
    article_name text, 
    unit_price integer,
    create_timestamp timestamp
)
;
create table ec_shop_sale (
    ec_shop_sale_id integer primary key autoincrement,
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
;
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
";
}



