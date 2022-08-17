# Project: Data Modeling with AWS Redshift

## Table of content

1. [Describe](#Describe)
2. [Project Structure](#project)
3. [How to run](#run)
4. [File Schema](#schema)

### Describe<a id='Describe'></a>

In this project, we will build a data lake in AWS S3 for Sparkify - a start up company. With raw data has been saved at another S3 bucket, we will perform ELT progress and save processed data into data lake.
We need to build a star schema database for song play dataset. 

The datalake contains 5 files: `songplays`, `users`, `songs`, `artists` and `time`.


### Project Structure<a id='project'></a>

```bash
├── ./etl.py # execute ELT progress
├── ./README.md
├── ./dl.cfg # AWS Key
├── ./EMR-implementation.ipynb # Notebook implement ELT progress in EMR services
```

### How to run<a id='run'></a>
Step 1: Update aws credentials in `dl.cfg`

Step 2: Run ELT process and save data into S3

```python
python etl.py
```


### File Schema in datalake<a id='schema'></a>

**Songs**

```bash
root
 |-- song_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- year: int (nullable = true)
 |-- duration: float (nullable = true)
 ```


**Artists***
 
 ```bash
root
 |-- artist_id: string (nullable = true)
 |-- artist_name: string (nullable = true)
 |-- artist_location: string (nullable = true)
 |-- artist_latitude: float (nullable = true)
 |-- artist_longitude: float (nullable = true)
 ```


**Users***
 
 ```bash
root
 |-- userId: int (nullable = true)
 |-- firstName: string (nullable = true)
 |-- lastName: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- level: string (nullable = true)
 ```


**Time***
 
 ```bash
root
 |-- start_time: string (nullable = true)
 |-- weekday: string (nullable = true)
 |-- year: int (nullable = true)
 |-- month: int (nullable = true)
 |-- week: int (nullable = true)
 |-- day: int (nullable = true)
 |-- hour: int (nullable = true)
 ```
 

**Songplays***
 
 ```bash
root
 |-- start_time: datetime (nullable = true)
 |-- userId: int (nullable = true)
 |-- level: string (nullable = true)
 |-- song_id: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- sessionId: int (nullable = true)
 |-- location: string (nullable = true)
 |-- userAgent: stribg (nullable = true)
 |-- year: int (nullable = true)
 |-- month: int (nullable = true)
 ```
