using KeyMapSync.Entity;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Test.Model.Postgres;

internal class TableManager
{
    public static List<string> GetCreateTableSqls()
    {
        var sql = @"
create table if not exists accounts(
    accounts_id serial8 not null primary key
    , journal_date date not null
    , accounts_name text not null
    , price int8 not null
    , remakrs text null
    , create_at timestamp default current_timestamp
)
;
create table if not exists ext_account_static_trans(
    ext_account_static_trans_id serial8 not null primary key
    , debit_accounts_id int8 not null
    , credit_accounts_id int8 not null
    , create_at timestamp default current_timestamp
)
;
create table if not exists sales(
    sales_id serial8 not null primary key
    , sales_date date not null
    , product_name text not null
    , price int8 not null
    , create_at timestamp default current_timestamp
)
;
create table if not exists payments(
    payments_id serial8 not null primary key
    , sales_id int8 not null
    , payment_date date not null
    , price int8 not null
    , accounts_name text not null
    , create_at timestamp default current_timestamp
)
;
create table if not exists journals(
    journals_id serial8 not null primary key
    , journal_date int8 not null
    , price int8 not null
    , create_at timestamp default current_timestamp
)
;
create table if not exists journal_details(
    journal_details serial8 not null primary key
    , journals_id int8 not null
    , debit_accounts_id int8 not null
    , credit_accounts_id int8 not null
    , price int8 not null
    , create_at timestamp default current_timestamp
)
";

        return sql.Split(";").ToList();
    }

    public static List<string> GetInitializeSqls()
    {
        var sql = @"
truncate table sales
;
truncate table payments
;
truncate table accounts
;
truncate table accounts_ext_trans
;
truncate table journals
;
truncate table journal_details
;
insert into sales(
    sales_date, product_name, price
)
values
    ('2022-01-01', 'apple', 100)
    , ('2022-02-01', 'orange', 200)
    , ('2022-03-01', 'coffee', 300)
    , ('2022-04-01', 'tea', 400)
;
insert into payments(
    sales_id, payment_date, price, accounts_name
)
select
    sales_id
    , sales_date::timestamp + '1 month'
    , price
    , 'cash' as accounts_name
from
    sales
";

        return sql.Split(";").ToList();
    }
}
