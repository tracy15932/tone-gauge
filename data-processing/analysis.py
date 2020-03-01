#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb  6 16:14:56 2020

@author: binnytsai
"""

import configparser as c
import os
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as f
import pandas

def main(sq):
    '''
    find the avg tone for each theme and source
    '''
    gkg_df = sq.read.parquet('s3a://gdelt2bucket/gkg/gkg.parquet/*')
    mention_df = sq.read.parquet("s3a://gdelt2bucket/mentions/mentions.parquet/*")
    event_df = sq.read.parquet("s3a://gdelt2bucket/event/event.parquet/*")
    
    '''
    mention inner join gkg
    '''
    join_df = mention_df.join(gkg_df,gkg_df.DocumentIdentifier == mention_df.MentionIdentifier, 'inner')
    join_df = join_df.select('MentionTimeDate','MentionSourceName',
                             'MentionDocTone',f.explode(f.split(f.col("Themes"), ";")).alias("Themes")).filter(join_df.Themes != '')
    join_df = join_df.withColumn('Themes',f.trim(join_df.Themes))
    
    
    '''
    filter out top 200 themes and top 100 sources from 2019
    '''
    top_theme = sq.read.parquet("s3a://gdelt2bucket/top_theme.parquet/*")
    top_theme_list = list(top_theme.select('Themes').toPandas()['Themes'])
    top_source = sq.read.parquet("s3a://gdelt2bucket/top_source.parquet/*")
    top_source_list = list(top_source.select('MentionSourceName').toPandas()['MentionSourceName'])
    
    join_df = join_df.filter(f.col('Themes').isin(top_theme_list))
    join_df = join_df.filter(f.col('MentionSourceName').isin(top_source_list))
    join_df = join_df.limit(1000)

    '''
    find the overall count and avg tone for each theme daily
    '''
    join_df1 = join_df.groupby('Themes','MentionTimeDate').agg(f.count('*').alias('count'),f.avg(f.col('MentionDocTone')).alias('AvgTone'))
    
    '''
    find the count and avg tone for each theme and each source daily
    '''
    join_df2 = join_df.groupby('Themes','MentionTimeDate','MentionSourceName').agg(f.count('*').alias('count'),f.avg(f.col('MentionDocTone')).alias('AvgTone'))
    
    '''
    mention inner join event
    '''
    mention_tone = mention_df.select('GLOBALEVENTID','MentionDocTone')
    event_avg_tone = mention_tone.groupby('GLOBALEVENTID').agg(f.avg(f.col('MentionDocTone')).alias('EventAvgTone'))
    em_join = event_df.join(event_avg_tone,'GLOBALEVENTID', 'inner')
    
    #get the top 1000 popular events
    em_join2 = em_join.sort(f.desc('NumMentions')).limit(1000)
    
    '''
    write to mysql db
    config db properties
    '''
    db_properties = {}
    config = configparser.ConfigParser()
    
    config.read("db_properties.ini")
    db_prop = config['mysql']
    db_url = db_prop['url']
    db_properties['username'] = db_prop['username']
    db_properties['password'] = db_prop['password']
    db_properties['url'] = db_prop['url']
    db_properties['driver'] = db_prop['driver']
    
    join_df.repartition(1000).write.format("jdbc").options(
        url = 'jdbc:mysql://gdeltdb.csjely5ddbj3.us-west-2.rds.amazonaws.com/gdelt_db',
        dbtable='gdelt_db.agg1',
        user= db_prop['username'],
        password= db_prop['password'],
        driver = db_prop['driver']).mode('append').save()
    
    join_df2.repartition(100).write.format("jdbc").options(
        url = 'jdbc:mysql://gdeltdb.csjely5ddbj3.us-west-2.rds.amazonaws.com/gdelt_db',
        dbtable='gdelt_db.agg2',
        user = db_prop['username'],
        password = db_prop['password'],
        driver = db_prop['driver']).mode('append').save()
    
    em_join2.repartition(1000).write.format("jdbc").options(
        url = 'jdbc:mysql://gdeltdb.csjely5ddbj3.us-west-2.rds.amazonaws.com/gdelt_db',
        dbtable='gdelt_db.event',
        user=db_prop['username'],
        password = db_prop['password'],
        driver = db_prop['driver']).mode('append').save()
    
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
        .appName("analysis") \
        .getOrCreate() 
        
    sc = spark.sparkContext
    sq = SQLContext(sc)
    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", aws_id)
    hadoop_conf.set("fs.s3a.secret.key", aws_key)
    
    main(sq)
    sc.stop()