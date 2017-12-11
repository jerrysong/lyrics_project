import collections
import constants
import happybase
import json
import os
import re
import heapq
import pyspark.conf
import pyspark.context

PROD_PATH = os.environ.get('PROD')
CLUSTER_CONFIG_PATH = PROD_PATH + '/conf/cluster_conf.json'
CLUSTER_CONFIG = json.loads(open(CLUSTER_CONFIG_PATH).read())
MASTER_HOST = CLUSTER_CONFIG['masterHost']
HBASE_PORT = CLUSTER_CONFIG['hbaseThriftPort']
HDFS_PORT = CLUSTER_CONFIG['hdfsMetadataPort']

APP_NAME = 'words_mapreduce'
