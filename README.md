# Kafka-Projects
In this project i loaded the data from mySQL into the a pandas dataframe. 
Where i have performed this following tasks.

* MySQL Table (Table should have some column like created_at or updated_at so that can be used for incremental read)
* Write a python script which is running in infinite loop and inserting 4-5 dummy/dynamically prepared records in MySQL Table
* Setup Confluent Kafka
* Create Topic
* Create json schema on schema registry (depends on what kind of data you are publishing in mysql table)
* Write a producer code which will read the data from MySQL table incrementally (hint : use and maintain create_at column)
* Producer will publish data in Kafka Topic
* Write consumer group to consume data from Kafka topic
* In Kafka consumer code do some changes or transformation for each record and write it in Cassandra table
