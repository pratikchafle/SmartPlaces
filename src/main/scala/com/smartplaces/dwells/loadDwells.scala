package com.smartplaces.dwells

// import of all the necessary classes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Base64
import java.nio.charset.StandardCharsets
import org.apache.spark.storage.StorageLevel


object loadDwells {

  def main(args: Array[String]) {


    //--------------------------------------------------------------
    // Creating Spark Session
    //--------------------------------------------------------------

    val spark = SparkSession
      .builder()
      .appName("Dwells")
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

    //--------------------------------------------------------------
    // Declaring variables
    //--------------------------------------------------------------

    val schEmpDB = "db_schema_empirix"
    val schVisitDB = "db_schema_visitation"
    val curatedDB = "db_curated_smartcities"
    val empirixConsolidated = "empirix_consolidation"
    val dwells = "tbl_dwells"
    val cells = "tbl_waterloo_cells"

    val appID = spark.sparkContext.applicationId
    val jobName = "Dwells"
    var dateTime = Calendar.getInstance.getTime
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    var currentTimestampString = timeFormat.format(dateTime)

    var jobStatus = "Failed"
    var recordCounts = 0

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

      val empirixDF = spark.read.table(s"$schEmpDB.$empirixConsolidated").filter($"year" === s"$year" && $"month" === s"$month" && $"day" === s"$day")
        .withColumnRenamed("imsi", "user_id")
        .withColumn("date_dt", to_date(coalesce($"start_timestamp", $"end_timestamp")))
        .withColumn("start_dt", coalesce($"start_timestamp", $"end_timestamp"))
        .withColumn("cell_id", coalesce($"start_source_cell_id", $"end_source_cell_id"))
        .select($"user_id", $"date_dt", $"start_dt", $"cell_id", $"year", $"month", $"day")


      val cellsDF = spark.read.table(s"$schVisitDB.$cells").withColumnRenamed("source_cell_id","cell_id").select($"cell_id").distinct()

      //Creating dataframe for Events Table
      val eventsDF = empirixDF.join(broadcast(cellsDF), empirixDF.col("cell_id") === cellsDF.col("cell_id") )
        .drop(cellsDF.col("cell_id"))


      //Creating dataframe for MicroDwells Table

      val microStep1DF = eventsDF.as("df1").join(eventsDF.as("df2"), $"df1.user_id" === $"df2.user_id" && $"df1.date_dt" === $"df2.date_dt")
        .where($"df2.start_dt" >= from_unixtime(unix_timestamp($"df1.start_dt").minus(45 * 60), "YYYY-MM-dd HH:mm:ss") && $"df2.start_dt" <= from_unixtime(unix_timestamp($"df1.start_dt").minus(1 * 60), "YYYY-MM-dd HH:mm:ss"))
        .select($"df1.date_dt", $"df1.user_id", $"df1.start_dt".as("time_1"), $"df2.start_dt".as("time_2"))

      val microStep2DF = microStep1DF.groupBy($"date_dt", $"user_id", $"time_1").agg(min($"time_2").alias("first_event"))

      val microStep3DF = microStep2DF.groupBy($"date_dt", $"user_id", $"first_event").agg(max($"time_1").alias("last_event"))

      val microDwellsDF = microStep3DF.withColumn("counter", row_number().over(Window.partitionBy(col("date_dt"), col("user_id")).orderBy(col("first_event"))))


      //Creating dataframe for MacroDwells Table

      val macroStep1DF = microDwellsDF.as("df1").join(microDwellsDF.as("df2"), $"df1.user_id" === $"df2.user_id" && $"df1.date_dt" === $"df2.date_dt").select($"df1.date_dt", $"df1.user_id", $"df1.first_event", $"df1.last_event", $"df1.counter", $"df2.first_event".as("first_event_2"), $"df2.last_event".as("last_event_2"), $"df2.counter".as("counter_2")).filter($"df2.counter".minus(1) === $"counter" && $"df1.last_event" >= $"df2.first_event")

      val macroStep2DF = macroStep1DF.withColumn("new_row", row_number().over(Window.partitionBy(col("date_dt"), col("user_id")).orderBy(col("first_event")))).where(macroStep1DF.col("counter_2").isNotNull)

      val ldDF = macroStep2DF.withColumn("dwell_number", $"new_row".minus($"counter"))

      val macroDwellsDF = ldDF.groupBy($"date_dt", $"user_id", $"dwell_number").agg(max($"last_event_2").alias("last_event"), min($"first_event").alias("first_event"))


      //Creating dataframe for Dwells Table
      val dwellsInitialDF = microDwellsDF.as("micro").join(macroDwellsDF.as("macro"), $"micro.date_dt" === $"macro.date_dt" && $"micro.first_event" <= $"macro.first_event" && $"micro.last_event" <= $"macro.last_event", "left_outer").select($"micro.date_dt", $"micro.user_id", coalesce($"macro.first_event", $"micro.first_event").as("first_event"), coalesce($"macro.last_event", $"micro.last_event").as("last_event")).distinct()

      val dwellsDF = dwellsInitialDF.withColumn("load_timestamp", lit(s"$currentTimestampString"))
        .withColumn("year", lit(s"$year"))
        .withColumn("month", lit(s"$month"))
        .withColumn("day", lit(s"$day"))

      dwellsDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

      // Dropping the partitions if exist
      val dwellsTruncateSql = s"ALTER TABLE $curatedDB.$dwells DROP IF EXISTS PARTITION(year =\42$year\42,month=\42$month\42,day=\42$day\42)"

      spark.sql(dwellsTruncateSql)

      //Inserting the data into Target Dwells Table

      dwellsDF.write.insertInto(s"$curatedDB.$dwells")

      recordCounts = dwellsDF.count().asInstanceOf[Int]

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

