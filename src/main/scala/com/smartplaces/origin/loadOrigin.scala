package com.smartplaces.origin

// import of all the necessary classes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Base64
import java.nio.charset.StandardCharsets
import org.apache.spark.storage.StorageLevel

object loadOrigin {

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
    val msoaCells = "tbl_msoa_cells"
    val originMsoa = "tbl_origin_msoa"

    val appID = spark.sparkContext.applicationId
    val jobName = "Origin"
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
      val previousEventsDF = empirixDF.join(dwellsDF, empirixDF.col("user_id") === dwellsDF.col("user_id") && empirixDF.col("date_dt") === dwellsDF.col("date_dt"))
        .filter(empirixDF.col("start_dt") < dwellsDF.col("first_event") && empirixDF.col("start_dt") > from_unixtime(unix_timestamp(dwellsDF.col("first_event")).minus(2 * 60 * 60), "YYYY-MM-dd HH:mm:ss"))
        .select(empirixDF.col("date_dt"), empirixDF.col("user_id"), empirixDF.col("start_dt"), empirixDF.col("cell_id"), dwellsDF.col("first_event"), dwellsDF.col("last_event"))

      //Creating dataframe for All MSOA Cell Table excluding Waterloo MSOA
      val allCellsDF = spark.read.table(s"$schVisitDB.$msoaCells").filter($"msoa" =!= lit("E02006801"))
        .withColumnRenamed("source_cell_id","cell_id").select($"cell_id",$"msoa").distinct()


      //Creating dataframe for Origin Table
      val originStep1DF = previousEventsDF.join(broadcast(allCellsDF), previousEventsDF.col("cell_id") === allCellsDF.col("cell_id")).filter($"start_dt" < from_unixtime(unix_timestamp($"first_event").minus(15 * 60), "YYYY-MM-dd HH:mm:ss")).groupBy(previousEventsDF.col("date_dt"), $"user_id", $"first_event", $"last_event", previousEventsDF.col("cell_id")).agg(count("*").alias("number_events"))

      val originStep2DF = originStep1DF.withColumn("max_events", row_number().over(Window.partitionBy(col("date_dt"), col("user_id"), col("first_event")).orderBy(col("number_events").desc)))

      val originDF = originStep2DF.filter(originStep2DF.col("max_events") === 1).drop("max_events").drop("number_events")

      //Creating dataframe for All MSOA Cell Table
      val allCellsMsoaDF = spark.read.table(s"$schVisitDB.$msoaCells").withColumnRenamed("source_cell_id","cell_id").select($"cell_id",$"msoa").distinct()

      //Creating dataframe for Origin Msoa Table
      val originMsoaStep1DF = dwellsDF.join(originDF, dwellsDF.col("user_id") === originDF.col("user_id") && dwellsDF.col("date_dt") === originDF.col("date_dt") && dwellsDF.col("first_event") === originDF.col("first_event"), "left_outer").select(dwellsDF.col("date_dt"), dwellsDF.col("user_id"), dwellsDF.col("first_event"), dwellsDF.col("last_event"), originDF.col("cell_id"))


      val originMsoaStep2DF = originMsoaStep1DF.join(broadcast(allCellsMsoaDF), originMsoaStep1DF.col("cell_id") === allCellsMsoaDF.col("cell_id"))
        .select(originMsoaStep1DF.col("date_dt"), originMsoaStep1DF.col("user_id"), originMsoaStep1DF.col("first_event"), originMsoaStep1DF.col("last_event").alias("last_known_event"), allCellsMsoaDF.col("msoa").alias("origin_msoa"))

      val originMsoaDF = originMsoaStep2DF.withColumn("load_timestamp", lit(s"$currentTimestampString"))
        .withColumn("year", lit(s"$year"))
        .withColumn("month", lit(s"$month"))
        .withColumn("day", lit(s"$day"))

      originMsoaDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

      // Dropping the partitions if exist
      val originMsoaTruncateSql = s"ALTER TABLE $curatedDB.$originMsoa DROP IF EXISTS PARTITION(year =\42$year\42,month=\42$month\42,day=\42$day\42)"

      spark.sql(originMsoaTruncateSql)

      //Inserting the data into Target Origin Msoa Table

      originMsoaDF.write.insertInto(s"$curatedDB.$originMsoa")

      recordCounts = originMsoaDF.count()

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

