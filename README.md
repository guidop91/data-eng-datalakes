# Data Engineering Nanodegree, Data Lake with Spark

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This codebase creates a database containing 5 tables in star schema,
that organizes data related to this music library, and user listening data. This data
has been extracted from these two next sources: 

- Song data: `s3://udacity-dend/song_data`

Sample: 
```
{
    "num_songs": 1, 
    "artist_id": "ARJIE2Y1187B994AB7", 
    "artist_latitude": null, 
    "artist_longitude": null, 
    "artist_location": "", 
    "artist_name": "Line Renaud",
    "song_id": "SOUPIRU12A6D4FA1E1", 
    "title": "Der Kleine Dompfaff", 
    "duration": 152.92036, 
    "year": 0
 }
```
- Log data: `s3://udacity-dend/log_data`

Sample: 

![log data sample](img/log-data.png)

First of all, using Spark, these s3 buckets are read and held in memory.
Then the data is extracted an introduced into 5 different tables in star-schema: 

- **Songplay Table**: this represents the only fact table in the star schema. It contains 
  data related to how users listen to music, including the time at which they listen to it, 
  their location, what song and artist (related with their IDs) the event relates to, and other
  pieces of information that can be used to analyze user listening activity. 

- **Users Table**: a dimension table, that holds user's data, including their first and last name,
  their gender and whether or not they're subscribed. 

- **Songs Table**: a dimension table, that holds songs details, including the title, it's 
  contributing artist, the duration, the year of its release, etc. 

- **Artist Table**: a dimension table, that holds aritst details, including their name and 
  their location. 

- **Time Table**: a dimension table, that holds many different ways of interpreting a timestamp, 
  like a weekday, hour, month, day of month, etc. 

## Brief explanation of each file in this repository

- `dl.cfg`: Config file where credentials and other configurations for AWS
  access are entered.

- `etl.py`: performs the bulk of the work, analyzing the files cited above, parsing the data, and 
  inserting it into parquet files in S3, while giving a progress in the console. 

## Running this project

1. Create an AWS account and get credentials for the IAM User. These will include
   an API key and a secret. 
2. Copy these values to the `dl.cfg` file, it will look something like this: 

```
[DEFAULT]
AWS_ACCESS_KEY_ID=akeyofletterandnumbers
AWS_SECRET_ACCESS_KEY=morelettersandnumbers
```

3. Run the `etl.py` and wait for the script to finish. This will connect to the EMR cluster
   and perform the copying of the data in the buckets to the 5 tables in parquet structure.