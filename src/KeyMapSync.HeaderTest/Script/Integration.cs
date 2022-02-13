namespace KeyMapSync.HeaderTest.Script;

public static class Integration
{
    public static string InitializeSql => @"
drop table if exists integration_sale
;
drop table if exists integration_sale_detail
;
drop table if exists integration_sale_detail_ext_ec_shop_article
;
drop table if exists integration_sale_detail_ext_store_sale_detail
;
create table integration_sale (
    integration_sale_id integer primary key autoincrement,
    shop_id integer, 
    sale_date date
)
;
create table integration_sale_detail (
    integration_sale_detail_id integer primary key autoincrement,
    integration_sale_id integer,
    article_name text,
    unit_price integer, 
    quantity integer, 
    price integer
)
;
create table integration_sale_detail_ext_ec_shop_article (
    integration_sale_detail_id integer primary key,
    ec_shop_article_id integer
)
;
create table integration_sale_detail_ext_store_sale_detail (
    integration_sale_detail_id integer primary key,
    store_article_id integer, 
    remarks text
)
;";
}