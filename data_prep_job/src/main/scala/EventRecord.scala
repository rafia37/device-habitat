import org.apache.spark.sql.SparkSession
//import org.apache.spark._
//import org.apache.spark.SparkContext._
import java.time._


val spark = SparkSession.builder.appName("test_job").getOrCreate()


// A case class for input data
case class EventRecord(
                        deviceId: String,
                        startTime: Long,
                        duration: Long,
                        latitude: Double,
                        longitude: Double
                      )

// A case class for output result
case class OutputRecord (
                          deviceId: String,
                          numHours: Int
                        )

//Reading in data
val data = spark.read.table("irys_lake_dev.historical_visits_1028").filter("devicePartition=1 and eventDate=date('2021-09-07')")

//Converting it to a dataset
val dataset = data.select($"deviceId", $"event.startTime", $"event.duration", $"event.centroid.latitude", $"event.centroid.longitude").as[EventRecord]

//Function to get distinct hours of each device
def getHoursFromEvent (startTime: Long, duration: Long) : List[Int] = {
  //Getting hour of start time
  val stimeInstant = Instant.ofEpochMilli(startTime)
  val stimeHour = stimeInstant.atZone(ZoneOffset.UTC).getHour()

  //Extracting End time based on duration
  val etimeInstant = stimeInstant.plusSeconds(duration/1000)  //Potential division error
  val etimeHour = etimeInstant.atZone(ZoneOffset.UTC).getHour()

  //While loop to create the hour sequence
  val etime = if (stimeHour <= etimeHour) etimeHour else (etimeHour+24)  //upper limit of while condition
  val hoursSet = collection.mutable.Set[Int]()   //empty sequence to record the hours seen
  var st = stimeHour //counter variable for while loop
  while(st <= etime) {
    hoursSet += st
    st = st + 1
  }
  hoursSet.toList
}


//Grouping by device to get distinct hours of each device
val grouped = dataset.groupByKey(a => a.deviceId).mapGroups { case (key: String, values: Iterator[EventRecord]) =>
  val deviceId = key
  val hoursSet = scala.collection.mutable.Set[Int]()

  values.foreach {value =>
    val hours = getHoursFromEvent(value.startTime, value.duration)
    hoursSet ++= hours
  }

  OutputRecord(key, hoursSet.size)

}

spark.stop()




