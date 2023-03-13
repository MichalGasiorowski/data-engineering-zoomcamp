## Week 6 Homework 

In this homework, there will be two sections, the first session focus on theoretical questions related to Kafka 
and streaming concepts and the second session asks to create a small streaming application using preferred 
programming language (Python or Java).

### Question 1: 

**Please select the statements that are correct **
- Kafka Node is responsible to store topics
- Zookeeper is removed form Kafka cluster starting from version 4.0
- Retention configuration ensures the messages not get lost over specific period of time.
- Group-Id ensures the messages are distributed to associated consumers

#### Answer 1
Kafka Node is responsible to store topics - they can be distributed on multiple of them, depending on replication factor.
Zookeeper dependency is removed from version 2.8.0
Retention configuration ensures the messages not get lost over specific period of time - True. Retention defines how long 
kafka will keep the messages.
Group-Id determines the group consumer belong to - for con

**A, C, D** 

### Question 2: 

**Please select the Kafka concepts that support reliability and availability**

- Topic Replication
- Topic Paritioning
- Consumer Group Id
- Ack All

#### Answer 2

Topic Replication factor configures number of broker the topic will be copied. The higher the replication factor, the higher reliabilty.

**A,D** 
### Question 3: 

**Please select the Kafka concepts that support scaling**  

- Topic Replication
- Topic Paritioning
- Consumer Group Id
- Ack All

#### Answer 3
-- Topic Paritioning & Consumer Group Id supports scaling 
- Ack All is in **opposition to scaling**

** B, C ** 

### Question 4: 

**Please select the attributes that are good candidates for partitioning key. 
Consider cardinality of the field you have selected and scaling aspects of your application**  

- payment_type
- vendor_id
- passenger_count
- total_amount
- tpep_pickup_datetime
- tpep_dropoff_datetime

#### Answer 4
- vendor_id
- tpep_pickup_datetime
- tpep_dropoff_datetime

**B, C ** 

### Question 5: 

**Which configurations below should be provided for Kafka Consumer but not needed for Kafka Producer**

- Deserializer Configuration
- Topics Subscription
- Bootstrap Server
- Group-Id
- Offset
- Cluster Key and Cluster-Secret

#### Answer 5
Consumer need Deserializer config, topics for which it's Subscribed, group-id and these configurations are not needed for a Producer.
Cluster Key and Cluster-Secret are needed for Consumer and Producer.
Bootstrap Server is needed for Consumer and Producer.
Offset is specified - it's taken from Kafka server, there is auto.offset.reset config for offset reset strategies when offset does not exist on the server.

**A, B, D ** 


### Question 6:

Please implement a streaming application, for finding out popularity of PUlocationID across green and fhv trip datasets.
Please use the datasets [fhv_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) 
and [green_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green)

PS: If you encounter memory related issue, you can use the smaller portion of these two datasets as well, 
it is not necessary to find exact number in the  question.

Your code should include following
1. Producer that reads csv files and publish rides in corresponding kafka topics (such as rides_green, rides_fhv)
2. Pyspark-streaming-application that reads two kafka topics
   and writes both of them in topic rides_all and apply aggregations to find most popular pickup location.

   
## Submitting the solutions

* Form for submitting: https://forms.gle/rK7268U92mHJBpmW7
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 13 March (Monday), 22:00 CET


## Solution

We will publish the solution here after deadline
