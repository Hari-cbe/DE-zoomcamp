<h1>Week 3 Answers with SQL Queries </h1>
<h4>Question 1: What is count of records for the 2022 Green Taxi Data??</h4>
<i>840,402</i>
<hr>
<h4>Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?</h4>
<i> 0 MB for the External Table and 6.41MB for the Materialized Table</i><br>
<b> SQL : <br>
<ul>
<li>select distinct(PULocationID) from ny_taxi.green_taxi_data; </li>
<li>select distinct(PULocationID) from ny_taxi.green_taxi_data_native;</li>
</ul>
</b>
<hr>
<h4>Question 3: How many records have a fare_amount of 0? </h4>
<i> 1,622 </i> <br>
<b> SQL : <br>
<ul><li>select count(fare_amount) from ny_taxi.green_taxi_data where fare_amount = 0;</li></ul></b>
<hr>
<h4>Question 4: What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy) </h4>
<i> Partition by lpep_pickup_datetime Cluster on PUlocationID </i>
<hr>
<h4>Question 5:
Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?
</h4>
<i>12.82 MB for non-partitioned table and 1.12 MB for the partitioned table</i><br>
<b> SQL : <br>
<ul>
<li>CREATE OR REPLACE TABLE ny_taxi.green_taxi_data_parti<br>
 PARTITION BY lpep_pickup_datetime <br>
 CLUSTER BY PULocationID AS <br>
 SELECT 
 *
 FROM
 ny_taxi.green_taxi_data_native;</li>
 <li>SELECT distinct(PULocationID) from ny_taxi.green_taxi_data_native where  lpep_pickup_datetime >= '2022-06-01' and lpep_pickup_datetime <= '2022-06-30';</li>
 <li>SELECT distinct(PULocationID) from ny_taxi.green_taxi_data_parti where  lpep_pickup_datetime >= '2022-06-01' and lpep_pickup_datetime <= '2022-06-30';</li>
</ul></b>
<hr>
<h4>Question 6: Where is the data stored in the External Table you created
</h4>
<i>GCP Bucket</i>
<hr>
<h4>Question 7:
It is best practice in Big Query to always cluster your data:</h4>
<i>False</i>
