// Creating date sequence

val a = Seq.range(27,32)
val augustDates = a.map{day => s"2021-08-$day"}

val b = Seq.range(1,10)
val septemberDates = b.map{day => s"2021-09-$day"}

val dates: Seq[String] = augustDates ++ septemberDates


// Reading data in
val data = spark.read.table("irys_lake_dev.historical_visits_1028").filter("devicepartition=1 and event.movementlabel='R'")


// Staging data day by day

val dfs = dates.map {dt => data.filter(s"eventDate=date('$dt')").select($"deviceId", $"countryCode", $"event.starttime", $"event.duration", $"event.timeZone", $"event.numLocates", $"event.centroid.latitude", $"event.centroid.longitude", $"eventDate")}      //RETURNS dfs: Seq[org.apache.spark.sql.DataFrame]

/* New Schema
root
 |-- deviceId: string (nullable = true)
 |-- starttime: long (nullable = true)
 |-- duration: long (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
 |-- eventDate: date (nullable = true)
*/


// Combining all the data
val combined = dfs.reduce((x,y) => x.union(y))

// Calculating number of records per date
combined.groupBy("eventDate").agg(count("*").as("records")).show()


//calculating the number of days devices were seen
val daysSeen = combined.groupBy("deviceId").agg(count("*").as("records"), countDistinct($"eventDate").as("numDaysSeen"))

/* How daysSeen table should look
+--------------------+-------+-----------+
|            deviceId|records|numDaysSeen|
+--------------------+-------+-----------+
|06952034-4ea0-5d4...|     85|         13|
|07b23d8d-1215-5c9...|     92|         14|
|052d84fd-304f-5dc...|     37|         12|
|041ff8e6-a660-52b...|     39|          8|
|05337549-d66c-53b...|     76|         14|
|05684626-11c1-50c...|     86|         14|
|05875689-af65-56d...|    102|         14|
|054c9f93-0de4-547...|     96|         14|
|05c5a4bf-ca44-58b...|     60|         12|
|072f73d5-fb16-5b5...|     78|         14|
|06011c6a-f1ba-517...|     99|          8|
|04165586-2203-598...|    125|         14|
|06a7f631-4237-502...|     92|         14|
|0736fae2-fe16-5cb...|    139|         14|
|0487efca-43d4-53c...|     99|         14|
|04080f47-98dc-506...|     88|         14|
|0584336e-2932-5e7...|     71|         14|
|07b2e533-2289-58b...|    108|         14|
|04f74697-c81a-502...|     43|         12|
|073e070d-e426-50c...|    118|         14|
+--------------------+-------+-----------+
*/

//-------106,528 devices have 14 days of data--------

//-------40,041 devices have 14 days and more than 100 records of data---------
val highQualityDev = daysSeen.filter("numDaysSeen=14 and records>100")


//sampling 100 devices from the 40,041 devices
val sampleDevices = highQualityDev.select("deviceId").take(100)
val sampleDeviceSet = sampleDevices.map(a => a.getString(0)).toSet


//user defined function to only get the sample devices
val deviceLookupUDF = udf { (s:String) => sampleDeviceSet.contains(s) }


//Applying the function to combined table
val sampleDeviceVisits = combined.filter(deviceLookupUDF($"deviceId"))
//RETURNS sampleDeviceVisits: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]
// CONCERNS It returned a row instead of dataframe
// VALIDATING It 13,612 rows. Since there are a 100 devices and each of them has >100 records, it should have >10,000 so, in the ball park


//Making sure all the data is in a single file
val singlePartition = sampleDeviceVisits.repartition(1)


//writing to s3
singlePartition.write.option("header","true").csv("s3://irys-develop/rafia/sampleDev_14days")


//downloading file
/*
aws s3 ls s3://irys-develop/rafia/sampleDev_14days/  //This works
aws s3 cp s3://irys-develop/rafia/sampleDev_14days/*.csv .  //This doesn't



