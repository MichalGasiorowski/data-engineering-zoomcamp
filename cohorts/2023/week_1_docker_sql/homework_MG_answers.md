## Week 1 Homework

In this homework we'll prepare the environment 
and practice with Docker and SQL


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command

Which tag has the following text? - *Write the image ID to the file* 

- `--imageid string`
- `--iidfile string`
- `--idimage string`
- `--idfile string`

A1) **B)** 
- `--iidfile string`
```
docker build --help | grep "Write the image"
```
-->  --iidfile string          Write the image ID to the file

## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use pip list). 
How many python packages/modules are installed?

- 1
- 6
- 3
- 7

```
 docker run --rm -ti python:3.9 /bin/bash -c ' pip list --disable-pip-version-check | tail -n +3'
```
returns:

> pip        22.0.4
> 
> setuptools 58.1.0
> 
> wheel      0.38.4
to get line count : 
```
 docker run --rm -ti python:3.9 /bin/bash -c ' pip list --disable-pip-version-check | tail -n +3 | wc -l'
```
--> 3
A2) **C)**
# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from January 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

A3)

Run ingestion scripts for green taxi trips and lookup zones;
ingestion script was modified to pass datetime_columns parameter ( it's different than in yellow_taxi data, in lookup zones also there is no datetime columns )
```bash

URL_GREEN="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"
URL_ZONE_LOOKUP="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url=${URL_GREEN} \
    --datetime_columns=lpep_pickup_datetime,lpep_dropoff_datetime

docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=zone_lookup \
    --url=${URL_ZONE_LOOKUP}
```
## Question 3. Count records 

How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 20689
- 20530
- 17630
- 21090

A3) 
```sql
SELECT count(*) FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-01-15 00:00:00'
AND lpep_dropoff_datetime < '2019-01-16 00:00:00' ;
```

--> 20530 **B)**


## Question 4. Largest trip for each day

Which was the day with the largest trip distance
Use the pick up time for your calculations.

- 2019-01-18
- 2019-01-28
- 2019-01-15
- 2019-01-10
A4)
```sql
SELECT date(gtt.lpep_pickup_datetime) FROM green_taxi_trips gtt
WHERE gtt.trip_distance = (SELECT max(trip_distance) FROM green_taxi_trips);
```
"2019-01-15" -> **C)**

## Question 5. The number of passengers

In 2019-01-01 how many trips had 2 and 3 passengers?
 
- 2: 1282 ; 3: 266
- 2: 1532 ; 3: 126
- 2: 1282 ; 3: 254
- 2: 1282 ; 3: 274

A5) 
```sql
SELECT passenger_count, COUNT(*) FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-01-01 00:00:00'
 AND lpep_pickup_datetime < '2019-01-02 00:00:00'
GROUP BY passenger_count;

```
| passenger_count   | count |
|----|-------|
| 0  | 21    |
| 1  | 12415 |
| 2  | 1282  |
| 3  | 254   |
| 4  | 129   |
| 5  | 616   |
| 6  | 273   |

2: 1282; 3: 254 -> **C)**

## Question 6. Largest tip

For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- Central Park
- Jamaica
- South Ozone Park
- Long Island City/Queens Plaza

A6)
```sql
SELECT gtt.tip_amount, zl."Zone" FROM green_taxi_trips gtt
JOIN zone_lookup zl ON gtt."DOLocationID" = zl."LocationID"
	WHERE gtt.tip_amount = (SELECT max(tip_amount) FROM green_taxi_trips
		JOIN zone_lookup ON "PULocationID" = "LocationID"
		WHERE "Zone" = 'Astoria');
```
88	"Long Island City/Queens Plaza"
-> **D)**

## Submitting the solutions

* Form for submitting: [form](https://forms.gle/EjphSkR1b3nsdojv7)
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 26 January (Thursday), 22:00 CET


## Solution

We will publish the solution here
