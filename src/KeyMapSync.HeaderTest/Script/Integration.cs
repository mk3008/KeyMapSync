﻿namespace KeyMapSync.HeaderTest.Script;

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
drop table if exists integration_sale_detail__map_ec_shop_sale_detail
;
drop table if exists integration_sale_detail__sync
;
drop table if exists integration_sale_detail__version
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
;";
}