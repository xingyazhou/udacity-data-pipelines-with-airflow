# Project: Data Pipelines with Airflow

## Introduction

<p>A music streaming company, Sparkify, decided that to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.</p>

<p>The goal is to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. Tests need to run against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.</p>

<p>The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.</p>

## Datasets

For this project, there are two datasets. Here are the s3 links for each:

>**s3://udacity-dend/song_data/**<br>
>**s3://udacity-dend/log_data/**

## Configuring the DAG

In the DAG, add default parameters according to these guidelines

1. The DAG does not have dependencies on past runs
2. On failure, the task are retried 3 times
3. Retries happen every 5 minutes
4. Catchup is turned off
5. Do not email on retry

In addition, configure the task dependencies so that after the dependencies are set, the graph view follows the flow shown in the image below.

![DAG!](./image/sparkify-dag.png "sparkify-dag")

## Author

**Xingya Zhou**

