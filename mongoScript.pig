/* author: Divyansh Srivastava */

/* Load Avro jars */
REGISTER /home/divyansh/pig-0.17.0/lib/avro-1.7.5.jar
REGISTER /home/divyansh/pig-0.17.0/lib/json-simple-1.1.jar
REGISTER /home/divyansh/pig-0.17.0/contrib/piggybank/java/piggybank.jar

/* MongoDB libraries */
REGISTER /home/divyansh/pig-0.17.0/lib/mongo-java-driver-3.5.0.jar
REGISTER /home/divyansh/pig-0.17.0/lib/mongo-hadoop-pig-2.0.2.jar
REGISTER /home/divyansh/pig-0.17.0/lib/mongo-hadoop-core-2.0.2.jar
REGISTER /home/divyansh/pig-0.17.0/lib/mongo-hadoop-2.0.2.jar

/* Set speculative execution off so we don't have the chance of duplicate records in Mongo */
set mapred.map.tasks.speculative.execution false
set mapred.reduce.tasks.speculative.execution false
define MongoStorage com.mongodb.hadoop.pig.MongoStorage(); /* Shortcut */
set default_parallel 7 



/* Loading data */

/* ArrDepDelays.AirportPerformance MR output */
airline_dep_delay= LOAD '/output_departure_airline_delay/part-r-00000' USING PigStorage(',') AS (airline:chararray, origin:chararray, delay:int);

/* monthwiseAvgFlightDelay.AverageFlightDelayByMonth  MR output */

monthly_avg_delay= LOAD '/output_monthly_airline_avg/part-r-00000' USING PigStorage(',') AS (airline:chararray, month:chararray, delay:int);

/* weeklyFlightDelay.AverageFlightDelayByWeek MR output*/

weekly_dep_delay= LOAD '/output_weekly_airline_avg/part-r-00000' USING PigStorage(',') AS (airline:chararray, week:chararray, delay:int);

/* avgflightdelay.AverageFlightDelay MR output */
avg_flight_delay= LOAD '/output_avgFlightdelay_arrdep_airline/part-r-00000' USING PigStorage(',') AS (airline:chararray, origin:chararray,destination:chararray, delay:int);

/* Storing data to mongodb */

STORE airline_dep_delay INTO 'mongodb://localhost/flight.airline_dep_delay'
    USING com.mongodb.hadoop.pig.MongoStorage();

STORE monthly_avg_delay INTO 'mongodb://localhost/flight.monthly_avg_delay'
    USING com.mongodb.hadoop.pig.MongoStorage();

STORE weekly_dep_delay INTO 'mongodb://localhost/flight.weekly_dep_delay'
    USING com.mongodb.hadoop.pig.MongoStorage();

STORE avg_flight_delay INTO 'mongodb://localhost/flight.avg_flight_delay'
    USING com.mongodb.hadoop.pig.MongoStorage();




