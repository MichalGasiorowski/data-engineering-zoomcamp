## Week 4 Homework 

In this homework, we'll use the models developed during the week 4 videos and enhance the already presented dbt project using the already loaded Taxi data for fhv vehicles for year 2019 in our DWH.

We will use the data loaded for:

* Building a source table: `stg_fhv_tripdata`
* Building a fact table: `fact_fhv_trips`
* Create a dashboard 

If you don't have access to GCP, you can do this locally using the ingested data from your Postgres database
instead. If you have access to GCP, you don't need to do it for local Postgres -
only if you want to.

> **Note**: if your answer doesn't match exactly, select the closest option 

Load the data fro green, yellow, fhv for 2019 years
Updated the script to laod:  [etl_web_to_gcs.py](..%2Fweek_2_workflow_orchestration%2Fcode%2Fflows%2F02_gcp%2Fetl_web_to_gcs.py)
to make it possible to enforce schemas, since they are different. There are differences in column names ( case !), encoding ( 'latin1' for fhv-2020 ).
All the files are loaded to project gcs. The tables in BigQuery are created through BigQuery UI - it's faster this way.

Setup dbt cloud, the data for fhv 2019 & 2020 is already there from week3. Copy project files to 
```week_4_analytics_engineering/taxi_rides_ny```

Run dbt cloud, setup, connect it to BigQuery, Github.
Don't forget to set dataset region where it's supposed to be ( by default == US )
Copy [taxi_zone_lookup.csv](..%2F..%2F..%2Fweek_4_analytics_engineering%2Ftaxi_rides_ny%2Fdata%2Ftaxi_zone_lookup.csv)
    [seeds_properties.yml](..%2F..%2F..%2Fweek_4_analytics_engineering%2Ftaxi_rides_ny%2Fdata%2Fseeds_properties.yml)
to .seeds folder under dbt-models
Run dbt seed from dbt-console. ( don't forget to edit .gitignore if necessary )
It results in new table being created in BigQuery.

run ```dbt build --var 'is_test_run: false' ```

![dbt build](screenshots%2Fdbt_build_screenshot.png)

### Question 1: 

**What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)?** 

You'll need to have completed the ["Build the first dbt models"](https://www.youtube.com/watch?v=UVI30Vxzd6c) video and have been able to run the models via the CLI. 
You should find the views and models for querying in your DWH.

- 41648442
- 51648442
- 61648442
- 71648442


#### Answer ####  

```bigquery
SELECT count(*) FROM `magnetic-energy-375219.production.fact_trips` ;
```
returns 
61644369


#### Answer 1 
**C** 61648442

### Question 2: 

**What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos?**

You will need to complete "Visualising the data" videos, either using [google data studio](https://www.youtube.com/watch?v=39nLTs74A3E) or [metabase](https://www.youtube.com/watch?v=BnLkrA7a6gM). 

- 89.9/10.1
- 94/6
- 76.3/23.7
- 99.1/0.9

#### Answer ####  

Created the report for fact_trips, as in the video:

![Looker_studio_fact_trips_report.png](screenshots%2FLooker_studio_fact_trips_report.png)

As we can see from the pie chart, the distribution is 89.9/10.1
#### Answer 2
**A** 89.9/10.1


### Question 3: 

**What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)?**  

Create a staging model for the fhv data for 2019 and do not add a deduplication step. Run it via the CLI without limits (is_test_run: false).
Filter records with pickup time in year 2019.

- 33244696
- 43244696
- 53244696
- 63244696

#### Answer ####  
Run:
```shell
dbt build --var 'is_test_run: false'
```
in production Environment
The query to get the count for stg_fhv_tripdata view:
s
```bigquery
SELECT count(*) FROM `magnetic-energy-375219.production.stg_fhv_tripdata` 
WHERE pickup_datetime >= TIMESTAMP('2019-01-01')
AND   pickup_datetime <= TIMESTAMP('2020-01-01');
```
return 43244693

#### Answer 3
**B** 43244696

### Question 4: 

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?**  

Create a core model for the stg_fhv_tripdata joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run it via the CLI without limits (is_test_run: false) and filter records with pickup time in year 2019.

- 12998722
- 22998722
- 32998722
- 42998722


#### Answer ####  
Run:
```shell
dbt build --var 'is_test_run: false'
```
in production Environment


```bigquery
SELECT count(*) FROM `magnetic-energy-375219.production.fact_fhv_trips` 
WHERE pickup_datetime >= TIMESTAMP('2019-01-01')
AND   pickup_datetime < TIMESTAMP('2020-01-01');
```
return 22998722

#### Answer 4
**B** 22998722

### Question 5: 

**What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table?**

Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.

- March
- April
- January
- December
- 
#### Answer ####  

Create data source for fact_fhv_trips table:
![Looker_studio_fact_fhv_trips_datasource.png](screenshots%2FLooker_studio_fact_fhv_trips_datasource.png)

Created report with trips per month bar chart : 

![fhv_trips_report.png](screenshots%2Ffhv_trips_report.png)

As seen from the report, most of the trips for fhv data were in January.

#### Answer 5
**C** January

## Submitting the solutions

* Form for submitting: https://forms.gle/6A94GPutZJTuT5Y16
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 25 February (Saturday), 22:00 CET


## Solution

We will publish the solution here
