﻿namespace KeyMapSync.Test.Script;

public static class Integration
{
    public static string InitializeSql => @"
drop table if exists integration_sale
;
drop table if exists integration_sale_detail
;
drop table if exists integration_sale_detail_ext_ec_shop_sale_detail
;
drop table if exists integration_sale_detail_ext_store_sale_detail
;
create table integration_sale (
    integration_sale_id integer primary key autoincrement,
    sale_date date, 
    create_timestamp timestamp
)
;
create table integration_sale_detail (
    integration_sale_detail_id integer primary key autoincrement,
    integration_sale_id integer,
    name integer,
    unit_price integer, 
    quantity integer, 
    price integer
)
;
create table integration_sale_detail_ext_ec_shop_sale_detail (
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