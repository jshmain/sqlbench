#!/bin/bash
source ../tpcds-env.sh

impala-shell -q "invalidate metadata" -i ${IMPALA_HOST}
impala-shell -i ${IMPALA_HOST}  -d $TPCDS_DBNAME <<EOF
create table date_dim like tpcds_raw.date_dim stored as parquetfile;
insert overwrite table date_dim select * from tpcds_raw.date_dim;

create table time_dim like tpcds_raw.time_dim stored as parquetfile;
insert overwrite table time_dim select * from tpcds_raw.time_dim;

create table customer like tpcds_raw.customer stored as parquetfile;
insert overwrite table customer select * from tpcds_raw.customer;

create table customer_address like tpcds_raw.customer_address stored as parquetfile;
insert overwrite table customer_address select * from tpcds_raw.customer_address;

create table customer_demographics like tpcds_raw.customer_demographics stored as parquetfile;
insert overwrite table customer_demographics select * from tpcds_raw.customer_demographics;

create table household_demographics like tpcds_raw.household_demographics stored as parquetfile;
insert overwrite table household_demographics select * from tpcds_raw.household_demographics;

create table item like tpcds_raw.item stored as parquetfile;
insert overwrite table item select * from tpcds_raw.item;

create table promotion like tpcds_raw.promotion stored as parquetfile;
insert overwrite table promotion select * from tpcds_raw.promotion;

create table store like tpcds_raw.store stored as parquetfile;
insert overwrite table store select * from tpcds_raw.store;



create table call_center like tpcds_raw.call_center stored as parquetfile;
insert overwrite table call_center select * from tpcds_raw.call_center;

create table catalog_page like tpcds_raw.catalog_page stored as parquetfile;
insert overwrite table catalog_page select * from tpcds_raw.catalog_page;

create table catalog_returns like tpcds_raw.catalog_returns stored as parquetfile;
insert overwrite table catalog_returns select * from tpcds_raw.catalog_returns;

create table catalog_sales like tpcds_raw.catalog_sales stored as parquetfile;
insert overwrite table catalog_sales select * from tpcds_raw.catalog_sales;

create table income_band like tpcds_raw.income_band stored as parquetfile;
insert overwrite table income_band select * from tpcds_raw.income_band;

create table inventory like tpcds_raw.inventory stored as parquetfile;
insert overwrite table inventory select * from tpcds_raw.inventory;

create table reason like tpcds_raw.reason stored as parquetfile;
insert overwrite table reason select * from tpcds_raw.reason;

create table ship_mode like tpcds_raw.ship_mode stored as parquetfile;
insert overwrite table ship_mode select * from tpcds_raw.ship_mode;

create table store_returns like tpcds_raw.store_returns stored as parquetfile;
insert overwrite table store_returns select * from tpcds_raw.store_returns;

create table warehouse like tpcds_raw.warehouse stored as parquetfile;
insert overwrite table warehouse select * from tpcds_raw.warehouse;

create table web_page like tpcds_raw.web_page stored as parquetfile;
insert overwrite table web_page select * from tpcds_raw.web_page;

create table web_returns like tpcds_raw.web_returns stored as parquetfile;
insert overwrite table web_returns select * from tpcds_raw.web_returns;

create table web_sales like tpcds_raw.web_sales stored as parquetfile;
insert overwrite table web_sales select * from tpcds_raw.web_sales;

create table web_site like tpcds_raw.web_site stored as parquetfile;
insert overwrite table web_site select * from tpcds_raw.web_site;

show tables;
EOF
