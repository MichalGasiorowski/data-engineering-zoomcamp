## Week 5 Homework 

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the FHVHV 2021-06 data found here. [FHVHV Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz )


### Question 1: 

**Install Spark and PySpark** 

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?
- 3.3.2
- 2.1.4
- 1.2.3
- 5.4
</br></br>

#### Answer ####  
Java already installed, to make sure that openjdk-11 is installed, run: 

```shell
sudo add-apt-repository ppa:openjdk-r/ppa
sudo apt-get update
sudo apt install openjdk-11-jdk
```


```shell
michal@pop-os:~$ java -version
openjdk version "11.0.18" 2023-01-17
OpenJDK Runtime Environment (build 11.0.18+10-post-Ubuntu-0ubuntu122.04)
OpenJDK 64-Bit Server VM (build 11.0.18+10-post-Ubuntu-0ubuntu122.04, mixed mode, sharing)
michal@pop-os:~$ update-java-alternatives --list
java-1.11.0-openjdk-amd64      1111       /usr/lib/jvm/java-1.11.0-openjdk-amd64
```
Update and export JAVA_HOME : 

edit ~/.bashrc : 
```shell
export JAVA_HOME=$(dirname $(dirname `readlink -f /etc/alternatives/java`))
export PATH="${JAVA_HOME}/bin:${PATH}"
```    
& run
source ~/.bashrc

Install spark
```bash
wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
```
untar to SPARK_HOME ( "${HOME}/spark/spark-3.3.2-bin-hadoop3" )
add to .bashrc : 
```bash
export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"
```

source ~/.bashrc
to test that everything is working, run : 

`spark-shell`
and execute : 
```scala
val data = 1 to 10000
val distData = sc.parallelize(data)
distData.filter(_ < 10).collect()
```

```shell
michal@pop-os:~$ spark-shell
23/03/01 17:07:47 WARN Utils: Your hostname, pop-os resolves to a loopback address: 127.0.1.1; using 192.168.0.129 instead (on interface wlp4s0)
23/03/01 17:07:47 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
23/03/01 17:07:50 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Spark context Web UI available at http://192.168.0.129:4040
Spark context available as 'sc' (master = local[*], app id = local-1677686871318).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.3.2
      /_/
         
Using Scala version 2.12.15 (OpenJDK 64-Bit Server VM, Java 11.0.18)
Type in expressions to have them evaluated.
Type :help for more information.

scala> val data = 1 to 10000
data: scala.collection.immutable.Range.Inclusive = Range 1 to 10000

scala> val distData = sc.parallelize(data)
distData: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:24

scala> distData.filter(_ < 10).collect()
res0: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)

scala> 
```

install pyspark:
```shell
pip install pyspark
```
When running 
```jupyter
spark.version
```
in jupyter notebook, it correctly prints:
'3.3.2'

#### Answer 1 
**A** 3.3.2

### Question 2: 

**HVFHW June 2021**

Read it with Spark using the same schema as we did in the lessons.</br> 
We will use this dataset for all the remaining questions.</br>
Repartition it to 12 partitions and save it to parquet.</br>
What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.</br>


- 2MB
- 24MB
- 100MB
- 250MB
</br></br>

#### Answer ####  

```jupyter
!wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz
!gunzip fhvhv_tripdata_2021-06.csv.gz
```
schema is different, based on how header in csv look like:
```jupyterpython
!head fhvhv_tripdata_2021-06.csv
dispatching_base_num,pickup_datetime,dropoff_datetime,PULocationID,DOLocationID,SR_Flag,Affiliated_base_number
B02764,2021-06-01 00:02:41,2021-06-01 00:07:46,174,18,N,B02764
B02764,2021-06-01 00:16:16,2021-06-01 00:21:14,32,254,N,B02764
B02764,2021-06-01 00:27:01,2021-06-01 00:42:11,240,127,N,B02764
```

```jupyterpython
schema = types.StructType([
    #types.StructField('hvfhs_license_num', types.StringType(), True),
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True),
    types.StructField('Affiliated_base_number', types.StringType(), True)
])

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('fhvhv_tripdata_2021-06.csv')

df = df.repartition(12)

df.write.parquet('data/pq/fhvhv/2021/06/')
```

```jupyterpython
!ls -al --block-size=M data/pq/fhvhv/2021/06/*.parquet

-rw-r--r-- 1 michal michal 22M Mar  3 10:53 data/pq/fhvhv/2021/06/part-00000-90aef90d-98c3-4ac6-93f9-4073e7e2afc4-c000.snappy.parquet
-rw-r--r-- 1 michal michal 22M Mar  3 10:53 data/pq/fhvhv/2021/06/part-00001-90aef90d-98c3-4ac6-93f9-4073e7e2afc4-c000.snappy.parquet
-rw-r--r-- 1 michal michal 22M Mar  3 10:53 data/pq/fhvhv/2021/06/part-00002-90aef90d-98c3-4ac6-93f9-4073e7e2afc4-c000.snappy.parquet
...
```

#### Answer 2
**B** 24MB


### Question 3: 

**Count records**  

How many taxi trips were there on June 15?</br></br>
Consider only trips that started on June 15.</br>

- 308,164
- 12,856
- 452,470
- 50,982
</br></br>

#### Answer ####  

use pypark functions or sql : 
```jupyterpython
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .filter("pickup_date = '2021-06-15'") \
    .count()
```
, or : 
```jupyterpython
spark.sql("""
SELECT
    COUNT(1)
FROM 
    fhvhv_2021_06
WHERE
    to_date(pickup_datetime) = '2021-06-15';
""").show()
```
in both cases the reuslt is :
```text
+--------+
|count(1)|
+--------+
|  450872|
+--------+
```

#### Answer 3
**C** 452,470

### Question 4: 

**Longest trip for each day**  

Now calculate the duration for each trip.</br>
How long was the longest trip in Hours?</br>

- 66.87 Hours
- 243.44 Hours
- 7.68 Hours
- 3.32 Hours
</br></br>

#### Answer ####  

```jupyterpython
df \
    .withColumn('duration_hours', (df.dropoff_datetime.cast('long') - df.pickup_datetime.cast('long')) / 3600) \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .groupBy('pickup_date') \
        .max('duration_hours')  \
    .orderBy('max(duration_hours)', ascending=False) \
    .limit(5) \
    .show()

```
returns:
```text
+-----------+-------------------+
|pickup_date|max(duration_hours)|
+-----------+-------------------+
| 2021-06-25|   66.8788888888889|
| 2021-06-22| 25.549722222222222|
| 2021-06-27| 19.980833333333333|
| 2021-06-26| 18.197222222222223|
| 2021-06-23| 16.466944444444444|
+-----------+-------------------+
```
or in spark sql:
```sparksql
spark.sql("""
SELECT
    to_date(pickup_datetime) AS pickup_date,
    MAX((CAST(dropoff_datetime AS LONG) - CAST(pickup_datetime AS LONG)) / 3600) AS duration_hours
FROM 
    fhvhv_2021_06
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT 10;
""").show()
```


#### Answer 4
**A** 66.87 Hours

### Question 5: 

**User Interface**

 Spark’s User Interface which shows application's dashboard runs on which local port?</br>

- 80
- 443
- 4040
- 8080
</br></br>

Spark UI runs by default on port 4040, if thi port is taken, it tries to run on 4041.

#### Answer 5
**C** 4040

### Question 6: 

**Most frequent pickup location zone**

Load the zone lookup data into a temp view in Spark</br>
[Zone Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv)</br>

Using the zone lookup data and the fhvhv June 2021 data, what is the name of the most frequent pickup location zone?</br>

- East Chelsea
- Astoria
- Union Sq
- Crown Heights North
</br></br>

#### Answer ####  

```sparksql
spark.sql("""
SELECT
    pul.Zone,
    COUNT(1) AS PICKUP_COUNT
FROM
    fhvhv_2021_06 fhv LEFT JOIN zones pul ON fhv.PULocationID = pul.LocationID
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT 5;
""").show()
```

returns : 
```text
+-------------------+------------+
|               Zone|PICKUP_COUNT|
+-------------------+------------+
|Crown Heights North|      231279|
|       East Village|      221244|
|        JFK Airport|      188867|
|     Bushwick South|      187929|
|      East New York|      186780|
+-------------------+------------+
```
#### Answer 6
**D** Crown Heights North

## Submitting the solutions

* Form for submitting: https://forms.gle/EcSvDs6vp64gcGuD8
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 06 March (Monday), 22:00 CET


## Solution

We will publish the solution here
