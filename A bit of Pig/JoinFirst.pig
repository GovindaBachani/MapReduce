REGISTER file:/home/hadoop/lib/pig/piggybank.jar	

-- CSVLoader function similar to OpenCSV in the MapReduce Java Program.
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;	
  


-- Reduce Task are set to 10 same as setNumberReduceTask in Java Map Reduce
SET default_parallel 10;

-- Loading the FlightData from the CSV(i.e. Data03.csv)
FlightData = LOAD 's3://govinda.mapreduce/data03/data.csv' using CSVLoader();

-- parsing the flight Data for F1
F1 = FOREACH FlightData GENERATE $5 as TravelDate, $11 as Origin, 
	  $17 as Destination, $24 as DepartureTime, $35 as ArrivalTime, 
	  $37 as Delay, $41 as Cancelled, $43 as Diverted;

-- parsing the flight Data for F2
F2 = FOREACH FlightData GENERATE $5 as TravelDate, $11 as Origin, 
	  $17 as Destination, $24 as DepartureTime, $35 as ArrivalTime, 
	  $37 as Delay, $41 as Cancelled, $43 as Diverted;

-- filtering the unrequired tuples from both F1 and F2
F1_filtered = FILTER F1 BY (Origin eq 'ORD' AND Destination neq 'JFK') 
		AND (Cancelled eq '0' AND Diverted eq '0');
F2_filtered = FILTER F2 BY (Origin neq 'ORD' AND Destination eq 'JFK') 
		AND (Cancelled eq '0' AND Diverted eq '0');

-- Now We join the results on the basis of Travel Date and Destination for F1 
-- and Origin for F2 


JoinData = JOIN F1_filtered by (TravelDate, Destination), 
	        F2_filtered by (TravelDate, Origin);


-- Checking that Joined Data is having proper records satisfying the conditions.
-- ArrivalTime of f1($4) is less than Departure time of f2($11)
-- Checking the date range lies between June 2007 to May 2008.

FilteredData = FILTER JoinData BY (int)$4 < (int)$11
		AND (ToDate($0,'yyyy-MM-dd') < ToDate('2008-06-01','yyyy-MM-dd')) 
		AND (ToDate($0,'yyyy-MM-dd') > ToDate('2007-05-31','yyyy-MM-dd'))
		AND (ToDate($8,'yyyy-MM-dd') < ToDate('2008-06-01','yyyy-MM-dd'))
		AND (ToDate($8,'yyyy-MM-dd') > ToDate('2007-05-31','yyyy-MM-dd'));

-- Adding up the delay for flights F1 and F2.

FilteredData = FOREACH FilteredData GENERATE (float)($5 + $13) as TotalDelay;

-- Writing the average delay after grouping.

TotalResults = GROUP FilteredData all;
Average = FOREACH TotalResults GENERATE AVG(FilteredData.TotalDelay);

STORE Average into 's3://govinda.mapreduce/Pigoutput_NEW1';
