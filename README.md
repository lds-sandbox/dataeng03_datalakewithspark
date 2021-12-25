# Data Warehouse ETL using Python and Spark

## Introduction

This project demonstrates an ETL process that reads attribute information and log data using PySpark and writes the information into S3 buckets for analysis.  The data represents songs in a streaming service called Sparkify and log data that contains information about who listened to which songs and other related attributes.

This project was developed as part of the Udacity Data Engineering nanodegree program.

## How to install
There is no installation package.  The folder structure and all of the files can be downloaded from the repository and saved directly on a local computer.  The code expects a configuration file called 'dl.cfg' to be included.  The structure of the configuration file should be:

\[AWS\] <br>
AWS_ACCESS_KEY_ID=<br>
AWS_SECRET_ACCESS_KEY=<br>
S3_OUTPUT_BUCKET=<br>

## How to use
* All scripts expect an associated configuration file to house the IAM users keys and parameters for the S3 output bucket.
* The <span>etl.py</span> script is the main script that reads and process all of the files in the data folder

## Technologies used
package (version) <br>
python (3.6.3) <br>
datetime (3.6) <br>
os (3.6) <br>
pyspark (2.4.3) <br>
pandas (0.23.3) <br>
