using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PostgresSample;

internal class DbInitializer
{
    public static void Execute(IDbConnection cn)
    {
        Sqls.Split(";").ToList().ForEach(sql => cn.Execute(sql));
    }

    public static string Sqls => @"
--fish_sales
drop table if exists fish_sales
;
create table fish_sales (
      fish_sales_id serial8 primary key
    , sales_date date not null
    , fish_name text not null
    , price int8 not null
)
;
insert into fish_sales(
    sales_date, fish_name, price
)
values 
      ('2000-01-01', 'tuna', 200)
    , ('2000-01-01', 'squid', 400)
    , ('2000-02-01', 'tuna', -200)
;

--fruits_sales
drop table if exists fruits_sales
;
create table fruits_sales (
      fruits_sales_id serial8 primary key
    , sales_date date not null
    , fruits_name text not null
    , price int8 not null
    , delete_date date null
)
;
insert into fruits_sales(
    sales_date, fruits_name, price, delete_date
)
values 
      ('2000-01-01', 'apple', 100, '2000-02-01')
    , ('2000-01-01', 'orange', 300, null)
;

--integration_sales
drop table if exists integration_sales
;
create table integration_sales (
      integration_sales_id serial8 primary key
    , sales_date date not null
    , product_name text not null
    , price int8 not null
)";

}
