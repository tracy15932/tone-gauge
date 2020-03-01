#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 00:09:40 2020

@author: binnytsai
"""
import configparser as c
import os
import pyspark.sql
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as f
from pyspark.sql.types import FloatType, IntegerType
import re


def df_clean(df,header):
    '''
    split the columns with corresponding header
    '''
    for i in range(0,len(header)):
        split_col = f.split(df["_c0"],"\t")
        df = df.withColumn(header[i], split_col[i])
    
    df = df.drop('_c0')
    return df

def clean_word(df,col):  
    '''
    Extract Theme column and clean the words tax_ , wb_, and
    all numbers, 
    '''
    df = df.withColumn(col,f.regexp_replace(col,'TAX_',''))
    df = df.withColumn(col,f.regexp_replace(col,'WB_',''))
    df = df.withColumn(col,f.regexp_replace(col,r'\d+',' '))
    df = df.withColumn(col,f.regexp_replace(col,'_',' '))
   
    return df
    
 
def main(sq):
    
    mention_df = sq.read.csv('s3a://gdelt-open-data/v2/mentions/2018*.mentions.csv')
    gkg_df = sq.read.csv('s3a://gdelt-open-data/v2/gkg/2018*.gkg.csv')
    gkg_df1 = gkg_df.select('_c0')
    events_df = sq.read.csv('s3a://gdelt-open-data/v2/events/2018*.export.csv')
   
    mention_header=['GLOBALEVENTID','EventTimeDate','MentionTimeDate',
                     'MentionType','MentionSourceName','MentionIdentifier',
                     'SentenceID','Actor1CharOffset','Actor2CharOffset','ActionCharOffset',
                     'InRawText','Confidence','MentionDocLen','MentionDocTone','MentionDocTranslationInfo',
                     ]
    
    gkg_header=['GKGRECORDID','DATE','SourceCollectionIdentifier','SourceCommonName',
                'DocumentIdentifier','Counts','V2Counts','Themes','V2Themes',
                'Locations','V2Locations','Persons','V2Persons','Organizations'
                'V2Organizations','V2Tone','Dates','GCAM','SharingImage','RelatedImages',
                'SocialImageEmbeds','SocialVideoEmbeds','Quotations','AllNames','Amounts',
                'TranslationInfo']
   
    events_header = ['GLOBALEVENTID','SQLDATE', 'MonthYear', 'Year', 'FractionDate', 
              'Actor1Code', 'Actor1Name', 'Actor1CountryCode', 'Actor1KnownGroupCode',
              'Actor1EthnicCode', 'Actor1Religion1Code', 'Actor1Religion2Code', 
              'Actor1Type1Code','Actor1Type2Code', 'Actor1Type3Code', 'Actor2Code', 'Actor2Name', 
              'Actor2CountryCode', 'Actor2KnownGroupCode', 'Actor2EthnicCode', 
              'Actor2Religion1Code', 'Actor2Religion2Code', 'Actor2Type1Code',
              'Actor2Type2Code', 'Actor2Type3Code', 'IsRootEvent', 'EventCode', 
              'EventBaseCode', 'EventRootCode', 'QuadClass', 'GoldsteinScale', 
              'NumMentions', 'NumSources', 'NumArticles', 'AvgTone', 'Actor1Geo_Type', 
              'Actor1Geo_Fullname', 'Actor1Geo_CountryCode', 'Actor1Geo_ADM1Code', 
              'Actor1Geo_ADM2Code', 'Actor1Geo_Lat', 'Actor1Geo_Long', 'Actor1Geo_FeatureID', 
              'Actor2Geo_Type', 'Actor2Geo_Fullname', 'Actor2Geo_CountryCode', 
              'Actor2Geo_ADM1Code', 'Actor2Geo_ADM2Code', 'Actor2Geo_Lat', 'Actor2Geo_Long',
              'Actor2Geo_FeatureID', 'ActionGeo_Type', 'ActionGeo_Fullname', 
              'ActionGeo_CountryCode', 'ActionGeo_ADM1Code', 'ActionGeo_ADM2Code', 
              'ActionGeo_Lat', 'ActionGeo_Long', 'ActionGeo_FeatureID', 'Dateadded', 
              'Sourceurl']
    
    
    df1 = df_clean(mention_df,mention_header)
    df2 = df_clean(events_df,events_header)
    df3 = df_clean(gkg_df1,gkg_header)
    
    '''
    take out the columns we need for each table
    '''
    df1 = df1.select('GLOBALEVENTID','MentionIdentifier','MentionTimeDate','MentionSourceName','MentionDocTone')
    df2 = df2.select('GLOBALEVENTID','SQLDATE','Actor1Name','Actor2Name','EventRootCode','GoldsteinScale','NumMentions','AvgTone','ActionGeo_FullName','ActionGeo_Lat','ActionGeo_Long')
    df3 = df3.select('DocumentIdentifier','DATE','SourceCommonName','Themes')
    
    '''
    assign proper types to each col
    '''
    df1 = df1.withColumn("GLOBALEVENTID",df1["GLOBALEVENTID"].cast(IntegerType()))
    df1 = df1.withColumn("MentionTimeDate",f.to_date(f.unix_timestamp(f.col('MentionTimeDate'), 'yyyyMMddHHmmss').cast("timestamp")))
    df1 = df1.withColumn("MentionDocTone", df1["MentionDocTone"].cast(FloatType()))
  
    df2 = df2.withColumn('GLOBALEVENTID', df2["GLOBALEVENTID"].cast(IntegerType())) 
    df2 = df2.withColumn("SQLDATE",f.to_date(f.unix_timestamp(f.col('SQLDATE'), 'yyyyMMdd').cast("timestamp")))
    df2 = df2.withColumn('GoldsteinScale',df2['GoldsteinScale'].cast(FloatType()))
    df2 = df2.withColumn('NumMentions',df2['NumMentions'].cast(IntegerType()))
    df2 = df2.withColumn('ActionGeo_Lat',df2['ActionGeo_Lat'].cast(FloatType()))
    df2 = df2.withColumn('ActionGeo_Long',df2['ActionGeo_Long'].cast(FloatType()))

    df3 = clean_word(df3,'Themes')
    df3 = df3.withColumn("DATE",f.to_date(f.unix_timestamp(f.col('DATE'), 'yyyyMMddHHmmss').cast("timestamp")))

     
    '''
    save cleaned tables to s3
    '''
    df1.write.parquet("s3a://gdelt2bucket/mentions/mentions.parquet", mode = "append")
    df2.write.parquet("s3a://gdelt2bucket/event/event.parquet",mode = "append")
    df3.write.parquet("s3a://gdelt2bucket/gkg/gkg.parquet", mode = "append")
    
    
if __name__ == "__main__":
    '''
    config aws and read data from s3 open source into spark
    '''
    
    config = c.ConfigParser()
    config.read(os.path.expanduser('~/.aws/credentials'))
    
    try:
        aws_id = config.get('default', "aws_access_key_id")
        aws_key = config.get('default', "aws_secret_access_key")
    
    except c.ParsingError:
        print("Error on parsing file!")
    
    spark = SparkSession.builder \
        .appName("ingestion") \
        .getOrCreate() 
        
    sc = spark.sparkContext
    sq = SQLContext(sc)
    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", aws_id)
    hadoop_conf.set("fs.s3a.secret.key", aws_key)
    
    
    main(sq)
    sc.stop()
    print("success!")
   
    
    
    
    

        

    
    
