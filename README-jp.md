# 概要

**KeyMapSync** は会計処理を意識した軽量ETLライブラリです。

## デモ

売上という関心事を管理しているサブシステムが複数存在し、それぞれ設計思想で作成されていることはよくあります。

**青果サブシステム 売上伝票**（論理削除をサポート）
| sales_id(PK) | sales_date | article_name | price | is_deleted | delete_date |
|:---|:---| :---|---:| :---: |:---|
|1 |2000-01-01 |apple |100 |true |2000-02-01 |
|2 |2000-01-01 |orange |300 |false |NULL |

**魚屋サブシステム 売上伝票**（赤黒をサポート）
| sales_id(PK) |sales_date | article_name | price |
|:---|:---|:---|---:|
|1 |2000-01-01 |tuna |200 |
|2 |2000-01-01 |squid |400 |
|3 |2000-02-01 |tuna |-200 |

これを統一構造（会計サブシステム 売上伝票）で管理したい場合、KeyMapSyncを使用すれば簡単にデータの統合化ができます。

**会計サブシステム 売上伝票**（赤黒をサポート）
| sales_id(PK) |sales_date | article_name | price |
|:---|:---|:---|---:|
|1 |2000-01-01 |apple |100 |
|2 |2000-01-01 |orange |300 |
|3 |2000-01-01 |tuna |200 |
|3 |2000-01-01 |squid |400 |
|4 |2000-02-01 |apple |-100 |
|5 |2000-02-01 |tuna |-200 |

## 特徴

赤黒転送（変更が生じている場合、元データを相殺するデータを転送する）に対応しています。詳細特徴は以下の通り。

- データ転送先（Destination）からデータ転送元（Datasoutce）の逆引きが可能
- データ転送元（Datasoutce）は選択クエリによる加工に対応
- 差分転送が可能
- 赤黒転送が可能

## 制約

緩めではありますが、KeyMapSyncを利用するには以下の条件を満たす必要があります。

- データ転送元（Datasoutce）とデータ転送先（Destination）のテーブルは同一DBMSインスタンス内に存在すること
- データ転送元は主キーが設定されていること
- データ転送先はシーケンス列を持っていること

### 補足

>データ転送元（Datasoutce）とデータ転送先（Destination）のテーブルは同一DBMSインスタンス内に存在する

この条件を満たせない場合は、別のETLを使用して同一DBMSインスタンス内にデータを入れてからKeyMapSyncを使用してください。

## 実行環境

.NET

動作検証しているDBMSは PorsgeSQL。

## 使い方

デモの転送を行うには以下のような手順を踏みます。

### DBMSの準備（PostgreSQL）

転送元テーブル、転送元データを準備します。

```
--fish_sales
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
;--fruits_sales
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
```

続いて、転送先のテーブルも用意します。
```
create table integration_sales (
      integration_sales_id serial8 primary key
    , sales_date date not null
    , product_name text not null
    , price int8 not null
)
```

### データソースSQLの作成(PostgreSQL）

データソースを転送先(integration_sales)の構造に合わせる列マッピング処理を選択クエリで指定します。

fish_sales の場合、このような選択クエリになります。列に転送先の列名を記述するだけです。

```
--fish sales datasource
select
    sales_date
    , fish_name as product_name
    , price
from
    fish_sales
;
```

fruits_sales も同様に選択クエリを記述します。同テーブルは論理削除で実装されているため、**売上と取消の2クエリが必要**になることに注意ください。

```
--fruits sales datasource
select
    sales_date
    , fruits_name as product_name
    , price
from
    fruits_sales
;
--fruits sales cancel datasource
select
    delete_date as sales_date
    , fruits_name as product_name
    , price
from
    fruits_sales
where
    delete_date is not null  
```

## コード(C#)

転送設定をC#で記述します。









・・・・

## 備考

差分転送、赤黒転送を行うためにKeyMapSyncは**制御テーブル、一時テーブル、一時ビューを生成**していますので、権限のあるユーザでDBMSに接続する必要があります。








