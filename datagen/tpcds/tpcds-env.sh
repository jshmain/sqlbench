# Host of Impala daemon
export IMPALA_HOST=localhost

# top level directory for flat files in HDFS
export FLATFILE_HDFS_ROOT=/tmp/tpcds_new

# scale factor in GB
# SF 3000 yields ~1TB for the store_sales table
export TPCDS_SCALE_FACTOR=2

# the name for the tpcds database
export TPCDS_DBNAME=tpcds_pq
