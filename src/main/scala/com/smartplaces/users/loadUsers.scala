package com.smartplaces.users

// import of all the necessary classes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Base64
import java.nio.charset.StandardCharsets
import org.apache.spark.storage.StorageLevel
import java.time.format.DateTimeFormatter
import java.time.LocalDate

object loadUsers {
  def main(args: Array[String]){

    //--------------------------------------------------------------
    // Creating Spark Session
    //--------------------------------------------------------------

    val spark = SparkSession
      .builder()
      .appName("Users")
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

    val curatedDB = "db_curated_smartcities"
    val dwells = "tbl_dwells"
    val users = "tbl_users"

    val appID = spark.sparkContext.applicationId
    val jobName = "Visitation & Frequency (Users)"
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

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    val lastMonthDate = LocalDate.parse(runDate, formatter).minusDays(30).toString

    val previousYear = lastMonthDate.split("-")(0)

    val previousMonth = lastMonthDate.split("-")(1)

    val previousDay = lastMonthDate.split("-")(2)

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

      val dwellsDF = spark.read.table(s"$curatedDB.$dwells").where($"year" >= s"$previousYear" && $"year" <= s"$year").where($"month" >= s"$previousMonth" && $"month" <= s"$month").where($"day" >= s"$previousDay" && $"day" <= s"$day")

      val usersStg1DF = dwellsDF.groupBy($"date_dt", $"user_id").agg(count("*").alias("visits"))

      val usersStg2DF = usersStg1DF.groupBy("user_id").agg(sum("visits").alias("total_visits"), countDistinct("date_dt").alias("total_days"))

      val usersDF = usersStg2DF.withColumn("commuters", when(col("total_visits") > lit(6), 1).otherwise(0))
        .withColumn("load_timestamp", lit(s"$currentTimestampString"))
        .withColumn("year", lit(s"$year"))
        .withColumn("month", lit(s"$month"))
        .withColumn("day", lit(s"$day"))

      usersDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

      // Dropping the partitions if exist
      val usersTruncateSql = s"ALTER TABLE $curatedDB.$users DROP IF EXISTS PARTITION(year =\42$year\42,month=\42$month\42,day=\42$day\42)"

      spark.sql(usersTruncateSql)

      //Inserting the data into Target Users Table

      usersDF.write.insertInto(s"$curatedDB.$users")

      recordCounts = usersDF.count().asInstanceOf[Int]

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

