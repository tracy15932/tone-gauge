#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb  6 14:56:10 2020

@author: binnytsai
"""

import configparser as c
import os
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as f

def main(sq):
    '''
    find the top 100 sources
    '''
    mention_df = sq.read.parquet("s3a://gdelt2bucket/mentions/mentions.parquet/*")
    source_df = mention_df.select('MentionSourceName')
    final_source = source_df.groupby('MentionSourceName').count().sort(f.desc('count')).limit(100)
    final_source.write.parquet("s3a://gdelt2bucket/top_source.parquet",mode = 'overwrite')
    final_source.write.csv("s3a://gdelt2bucket/top_source.parquet",mode = 'append')

if __name__ == "__main__":
    '''
    config aws and read data from s3 into spark
    '''
    
    config = c.ConfigParser()
    config.read(os.path.expanduser('~/.aws/credentials'))
    
    try:
        aws_id = config.get('default', "aws_access_key_id")
        aws_key = config.get('default', "aws_secret_access_key")
    
    except c.ParsingError:
        print("Error on parsing file!")
    
    spark = SparkSession.builder \
        .appName("top-source") \
        .getOrCreate() 
        
    sc = spark.sparkContext
    sq = SQLContext(sc)
    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", aws_id)
    hadoop_conf.set("fs.s3a.secret.key", aws_key)
    
    main(sq)
    sc.stop()
   