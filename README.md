# Tone Gauge
> Measuring the heartbeat of the world

[Here](https://docs.google.com/presentation/d/1Vu55AhSqVXz_c8RTW-RVpyapguyn3cDvRjwUTnmgVxs/edit#slide=id.p) is my presentation.

The project is completed as part of the Insight Data Engineering progrom (Los Angeles, 2020). [Visit here](https://public.tableau.com/profile/binny.tsai#!/vizhome/Tone_15828784976640/Dashboard1) to interact with the dashboard.

## Introduction

News tone is defined as a quantitative measurement ranging between -10 to 10 on how people feel about the news event. Research has shown that news tone is correlated with the stock price which implies that the media sentiment is an important predictor of stock returns. Nevertheless, there are tons of news update every day. It is hard for investors to read every single news in order to understand the public attitude.  Therefore, to help the investors quickly gain insight on the global news tone, this project provides users with an interactive dashboard to quickly access the sentiment surrounding this topic from various news source. This grants the users an overview of the global news tone evolution. 

![](https://github.com/tracy15932/tone-gauge/blob/master/images/event_map.png "event-map")
![](https://github.com/tracy15932/tone-gauge/blob/master/images/distribution.png "distribution")

## Project Structure 

```
.
├── README.md
├── Ingestion
│   └── ingestion.py
├── database-scripts
│   └── gdelt_db.sql
├── data-processing
│   ├── analysis.py
│   ├── top-themes.py
│   └── top-sources.py
├── images
│   ├── event_map.png
│   ├── distribution.png
│   └── pipline.png
├── app
│   └── Tone.twb
└── run.sh
```
## Dataset

The Global Database of Events, Languages, and Tone (GDELT 2.0) database collects global news reports, and identifies the actors, locations, themes, organizations, sources, events invloved. It conisits with three tables: _Mention, Gkg, Event_. The database saves ~1.5TB data per year, and it updates every 15 minutes. 

## Architecture
![](https://github.com/tracy15932/tone-gauge/blob/master/images/pipline.png "pipline")

## Engineering challenges

### Data Cleanning

Columns from the raw data are not properly organized with the corresponding headers. In addition, the _theme_ column is encoded with specific taxonomy words. An example of WorldLanguage Japanese is encoded as the following:
```
WB_WORLDLANGUAGE_JAPANESE
```

### Big Data Manipulation

To run the large amount of data, I set up a spark cluster with a master node and six worker nodes. Due to Apache Spark has in-memory computation nature. The resource in the cluster may get bottlenecked. Here is an example of configuration setting that successfully run the ingestion job on the cluster.
```
spark-submit \
--master spark://ip-10-0-0-11:7077 \
--deploy-mode client \
--executor-memory 5G \
--driver-memory 4G \
--num-executors 3
--jars /usr/local/spark/jars/hadoop-aws-2.7.3.jar,/usr/local/spark/jars/aws-java-sdk-1.7.4.jar,/usr/local/spark/jars/mysql-connector-java-8.0.19.jar \
~/spark-job/ingestion.py
```
## Trade-offs

In order to configure and manipulate the large dataset, I decided to distribute the data processing jobs into two stages: data cleaning and data aggregation. The trade-off is that the run-time may take longer. However, distribution of job stages will enhance the stability and organization. 

## Running Tone Gauge

All raw data is extracted from S3 datasource and is cleaned to perform computation on tone analysis. Then the aggregation results are saved to MySQL database. Then, connect MySQL to Tableau to visualize the statistics.

Run the following script on spark cluster to start Tone Gauge
```
./run.sh
```



