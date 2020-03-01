#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb  6 11:18:40 2020

@author: tracytsai
"""

import configparser as c
import os
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as f

def main(sq):
    
    gkg_df = sq.read.parquet('s3a://gdelt2bucket/gkg/gkg.parquet/*')
    
    '''
    find the top 200 themes
    '''
    theme_df = gkg_df.select('Themes')
    theme_df = theme_df.select(f.explode(f.split(f.col("Themes"), ";")).alias("Themes"))
    theme_df = theme_df.filter(theme_df.Themes != '')
    theme_df = theme_df.withColumn('Themes',f.trim(f.col('Themes')))
    theme_final = theme_df.groupby('Themes').count().sort(f.desc('count')).limit(200)
    theme_final.write.parquet("s3a://gdelt2bucket/top_theme.parquet", mode = "overwrite")
    theme_final.write.csv("s3a://gdelt2bucket/top_theme.parquet", mode = "append")

    
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
        .appName("top-themes") \
        .getOrCreate() 
        
    sc = spark.sparkContext
    sq = SQLContext(sc)
    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", aws_id)
    hadoop_conf.set("fs.s3a.secret.key", aws_key)
    
    main(sq)
    sc.stop()
    print('done!')
   
    