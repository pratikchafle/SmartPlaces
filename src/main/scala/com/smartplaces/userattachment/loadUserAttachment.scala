package com.smartplaces.userattachment

// import of all the necessary classes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Base64
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.storage.StorageLevel

object loadUserAttachment {
  def main(args: Array[String]) {
    //--------------------------------------------------------------
    // Creating Spark Session
    //--------------------------------------------------------------

    val spark = SparkSession
      .builder()
      .appName("UserAttachement")
      .config("hive.compute.query.using.stats", "true")
      .config("hive.exec.parallel", "true")
      .config("hive.stats.fetch.column.stats", "true")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.stats.fetch.partition.stats", "true")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val blockSize = 1024 * 1024 * 512

    //--------------------------------------------------------------
    // Setting Spark Property
    //--------------------------------------------------------------

    spark.conf.set("dfs.blocksize", blockSize)
    spark.conf.set("parquet.block.size", blockSize)
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    spark.conf.set("spark.dynamicAllocation.enabled", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 52428800)
    spark.conf.set("spark.shuffle.service.enabled", "true")
    spark.conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "60s")
    spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "15m")

    //--------------------------------------------------------------
    // Declaring variables
    //--------------------------------------------------------------

    val schEmpDB = "db_schema_empirix"
    val schVisitDB = "db_schema_visitation"
    val curatedDB = "db_curated_smartcities"
    val empirixConsolidated = "empirix_consolidation"
    val dwells = "tbl_dwells"
    val users = "tbl_users"
    val railCells = "tbl_clapham_vauxhall_cells"
    val timeSeries = "tbl_time_series"
    val userAttachment = "tbl_users_attachement"


    val appID = spark.sparkContext.applicationId
    val jobName = "User Attachment"
    var dateTime = Calendar.getInstance.getTime
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    var currentTimestampString = timeFormat.format(dateTime)

    var jobStatus = "Failed"
    var recordCounts = 0L

    //Accepting Date parameter
    val runDate = args(0)
    val mysqlURL = args(1)
    val mysqlUserName = args(2)
    val mysqlPass = new String(Base64.getDecoder().decode(args(3)), StandardCharsets.UTF_8)
    val oozieWorkflowId = args(4)


    //-----------------------------------------------------------------
    // Reading Job rundate from Argument
    //-----------------------------------------------------------------

    val year = runDate.split("-")(0)

    val month = runDate.split("-")(1)

    val day = runDate.split("-")(2)

    //-----------------------------------------------------------------
    // Creating Initial Audit Entry
    //-----------------------------------------------------------------

    import java.sql._
    Class.forName("com.mysql.jdbc.Driver")
    var conn: Connection = null

    conn = DriverManager.getConnection(mysqlURL, mysqlUserName, mysqlPass)

    val insertSql = """insert into smartcities_audit (oozie_wf_id,app_id,job_name,start_timestamp,end_timestamp,status,load_date,record_count) values (?,?,?,?,?,'Started',?,0) """.stripMargin

    val preparedStmt: PreparedStatement = conn.prepareStatement(insertSql)
    preparedStmt.setString(1, s"$oozieWorkflowId")
    preparedStmt.setString(2, s"$appID")
    preparedStmt.setString(3, s"$jobName")
    preparedStmt.setString(4, s"$currentTimestampString")
    preparedStmt.setNull(5, java.sql.Types.TIMESTAMP)
    preparedStmt.setString(6, s"$runDate")

    preparedStmt.execute
    preparedStmt.close()

    //-----------------------------------------------------------------
    // Creating DataFrames
    //-----------------------------------------------------------------
    try {
      //Creating dataframe for Empirix Table
      val empirixDF = spark.read.table(s"$schEmpDB.$empirixConsolidated").filter($"year" === s"$year" && $"month" === s"$month" && $"day" === s"$day")
        .withColumnRenamed("imsi", "user_id")
        .withColumn("date_dt", to_date(coalesce($"start_timestamp", $"end_timestamp")))
        .withColumn("start_dt", coalesce($"start_timestamp", $"end_timestamp"))
        .withColumn("cell_id", coalesce($"start_source_cell_id", $"end_source_cell_id"))
        .select($"user_id", $"date_dt", $"start_dt", $"cell_id", $"year", $"month", $"day")

      //Creating dataframe for Dwells Table
      val dwellsDF = spark.read.table(s"$curatedDB.$dwells").filter($"year" === s"$year" && $"month" === s"$month" && $"day" === s"$day")

      //Creating dataframe for previousEvents Table
      val previousEventsRawDF = empirixDF.join(dwellsDF, empirixDF.col("user_id") === dwellsDF.col("user_id") && empirixDF.col("date_dt") === dwellsDF.col("date_dt"))
        .filter(empirixDF.col("start_dt") < dwellsDF.col("first_event") && empirixDF.col("start_dt") > from_unixtime(unix_timestamp(dwellsDF.col("first_event")).minus(2 * 60 * 60), "YYYY-MM-dd HH:mm:ss"))
        .select(empirixDF.col("date_dt"), empirixDF.col("user_id"), empirixDF.col("start_dt"), empirixDF.col("cell_id"), dwellsDF.col("first_event"), dwellsDF.col("last_event"))

      val previousEventsDF = previousEventsRawDF.repartition(100)
      previousEventsDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

      //Creating dataframe for RailTrips Table
      val railCellsDF = spark.read.table(s"$schVisitDB.$railCells").select($"cell_id").distinct()

      val previousEventsRailCellsDF = previousEventsDF.join(broadcast(railCellsDF), previousEventsDF.col("cell_id") === railCellsDF.col("cell_id")).select(previousEventsDF.col("cell_id"), previousEventsDF.col("date_dt"), previousEventsDF.col("start_dt"), previousEventsDF.col("user_id"))

      val railTripsDF = previousEventsRailCellsDF.join(dwellsDF, previousEventsRailCellsDF.col("user_id") === dwellsDF.col("user_id") && previousEventsRailCellsDF.col("date_dt") === dwellsDF.col("date_dt")).where(previousEventsRailCellsDF.col("start_dt") < dwellsDF.col("first_event") && previousEventsRailCellsDF.col("start_dt") > from_unixtime(unix_timestamp(dwellsDF.col("first_event")).minus(60 * 60), "YYYY-MM-dd HH:mm:ss")).select(previousEventsRailCellsDF.col("date_dt"), previousEventsRailCellsDF.col("user_id"), dwellsDF.col("first_event"), dwellsDF.col("last_event")).distinct().withColumn("rail_trips", lit(1))

      //Creating dataframe for OtherTrips Table
      val otherTripsDF = previousEventsDF.filter(previousEventsDF.col("start_dt") < previousEventsDF.col("first_event") && previousEventsDF.col("start_dt") > from_unixtime(unix_timestamp(previousEventsDF.col("first_event")).minus(15 * 60), "YYYY-MM-dd HH:mm:ss")).select($"date_dt", $"user_id", $"first_event", $"last_event").distinct().withColumn("other_mode", lit(1))

      //Creating dataframe for Users Table
      val usersDF = spark.read.table(s"$curatedDB.$users").filter($"year" === s"$year" && $"month" === s"$month" && $"day" === s"$day")

      //Creating dataframe for TimeSeries Table
      val timeSeriesDF = spark.read.table(s"$schVisitDB.$timeSeries").withColumn("new_ts", concat(lit(s"$runDate"), lit(" "), $"ts").cast(TimestampType)).select($"new_ts".as("ts"))

      //Creating dataframe for UserAttachment Table
      val userAttachmentStep1DF = dwellsDF.as("df1").join(usersDF.as("df2"), $"df1.user_id" === $"df2.user_id", "left_outer").select($"df1.date_dt", $"df1.user_id", $"df1.first_event", $"df1.last_event", $"df2.commuters", $"df2.total_visits")

      val userAttachmentStep2DF = userAttachmentStep1DF.as("df1").join(railTripsDF.as("df2"), $"df1.user_id" === $"df2.user_id" && $"df1.date_dt" === $"df2.date_dt" && $"df1.first_event" === $"df2.first_event" && $"df1.last_event" === $"df2.last_event", "left_outer").select($"df1.date_dt", $"df1.user_id", $"df1.first_event", $"df1.last_event", $"df1.commuters", $"df1.total_visits", $"df2.rail_trips")

      val userAttachmentStep3DF = userAttachmentStep2DF.as("df1").join(otherTripsDF.as("df2"), $"df1.user_id" === $"df2.user_id" && $"df1.date_dt" === $"df2.date_dt" && $"df1.first_event" === $"df2.first_event" && $"df1.last_event" === $"df2.last_event", "left_outer").select($"df1.date_dt", $"df1.user_id", $"df1.first_event", $"df1.last_event", $"df1.commuters", $"df1.total_visits", $"df1.rail_trips", $"df2.other_mode")

      val userAttachmentStep4DF = userAttachmentStep3DF.join(broadcast(timeSeriesDF)).filter($"ts" > $"first_event" && $"last_event" > from_unixtime(unix_timestamp($"ts").minus(14 * 60), "YYYY-MM-dd HH:mm:ss"))

      val userAttachmentStep5DF = userAttachmentStep4DF.withColumn("dwell_time", (unix_timestamp($"ts") - unix_timestamp($"first_event")) / lit(60))
        .withColumn("railTrips", when(col("rail_trips") === lit(1), 1).otherwise(0))
        .withColumn("other_trips", when(col("rail_trips") === lit(1), 0).when(col("other_mode").isNull, 0).otherwise(1))
        .withColumn("tube_trips", when(col("rail_trips").isNull && col("other_mode").isNull, 1).otherwise(0))
        .select($"date_dt", $"ts".as("timeband"), $"user_id", $"total_visits", $"commuters", $"first_event", $"last_event", $"dwell_time", $"rail_trips", $"other_trips", $"tube_trips")

      //Creating dataframe for Final Output
      val userAttachmentDF = userAttachmentStep5DF.withColumn("load_timestamp", lit(s"$currentTimestampString"))
        .withColumn("year", lit(s"$year"))
        .withColumn("month", lit(s"$month"))
        .withColumn("day", lit(s"$day"))


      userAttachmentDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

      // Dropping the partitions if exist
      val userAttachmentTruncateSql = s"ALTER TABLE $curatedDB.$userAttachment DROP IF EXISTS PARTITION(year =\42$year\42,month=\42$month\42,day=\42$day\42)"

      spark.sql(userAttachmentTruncateSql)

      //Inserting the data into Target Users Table

      userAttachmentDF.write.insertInto(s"$curatedDB.$userAttachment")

      recordCounts = userAttachmentDF.count()

      jobStatus = "Completed"
    }
    finally {

      dateTime = Calendar.getInstance.getTime
      currentTimestampString = timeFormat.format(dateTime)

      if (jobStatus == "Completed") {

        val updateSql = """update smartcities_audit  set status=? , end_timestamp=? ,record_count=?  WHERE app_id=?  """.stripMargin
        val preparedStmt: PreparedStatement = conn.prepareStatement(updateSql)
        preparedStmt.setString(1, s"$jobStatus")
        preparedStmt.setString(2, s"$currentTimestampString")
        preparedStmt.setString(3, s"$recordCounts")
        preparedStmt.setString(4, s"$appID")
        preparedStmt.executeUpdate
        preparedStmt.close()
        conn.close()
        spark.stop()
      }
      else {
        val updateSql = """update smartcities_audit  set status=? , end_timestamp=?   WHERE app_id=?  """.stripMargin
        val preparedStmt: PreparedStatement = conn.prepareStatement(updateSql)
        preparedStmt.setString(1, s"$jobStatus")
        preparedStmt.setString(2, s"$currentTimestampString")
        preparedStmt.setString(3, s"$appID")
        preparedStmt.executeUpdate
        preparedStmt.close()
        conn.close()
        spark.stop()

      }
    }
  }
}

