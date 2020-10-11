package noaa

import java.io.{File, IOException}
import java.net.URL
import java.text.SimpleDateFormat

import org.apache.commons.io.FileUtils
import org.apache.commons.net.ftp.{FTP, FTPClient, FTPFile, FTPFileFilters}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GCCNDailyCompared extends App {

  def createRDD(outPath: String, yearFile: String): RDD[GHCNData] = {
    val lines = sc.textFile(outPath + "/" + yearFile)
    val rdd = lines.map { line =>
      val e = line.split(",")
      if (e.length > 7)
        GHCNData(
          e(0), toDate(e(1), "yyyyMMdd"), e(2),
          toIntOrNull(e(3)), toIntOrNull(e(4)), toIntOrNull(e(5)), toIntOrNull(e(6)),
          toHHMM(e(7)))
      else
        GHCNData(
          e(0), toDate(e(1), "yyyyMMdd"), e(2),
          toIntOrNull(e(3)), toIntOrNull(e(4)), toIntOrNull(e(5)), toIntOrNull(e(6)),
          None)
    } //.cache()
    rdd
  }

  def toDate(s: String, sformat: String): java.sql.Date = {
    val dateFormat = new SimpleDateFormat(sformat)
    val date: java.util.Date = dateFormat.parse(s)
    val d: java.sql.Date = new java.sql.Date(date.getTime())
    d
  }

  def toHHMM(s: String): Option[String] = {
    if (s.trim.length > 0) Some(s) else None
  }

  def toIntOrNull(s: String): Option[Int] = {
    try {
      Some(s.trim.toInt)
    } catch {
      case e: Exception => None
    }
  }

  def by_year(server: String, inPath: String, data: String, outPath: String): Unit = {
    // /media/$user/S1500GB/climate/ftp.ncdc.noaa.gov/ghcn/by_year
    val filespec = outPath + data
    val testFile = new File(filespec)
    if (!testFile.exists()) {
      val url = "ftp://" + server + "/" + inPath + "/" + data
      println(url)
      FileUtils.copyURLToFile(new URL(url), new File(filespec))
    } else println(s"File $data exists in $outPath")
  }

  val conf = new SparkConf()
    .setAppName("Global Daily Weather Data")
    .setMaster("local[*]")

  // sc is the SparkContext, while sqlContext is the SQLContext.
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  /*
     * In Spark 2, you need to import the implicits from the SparkSession:
     */
  val spark = SparkSession.builder().appName("GHCNDaily").getOrCreate()

  val user = "anonymous"
  val pass = "guest" // or your email address
  val server = "ftp.ncdc.noaa.gov"

  //  val outPath = "/home/sprk/tmp/"
  val outPath = "/media/datalake/" // important last backslash ...
  val inPath = "pub/data/ghcn/daily/by_year"

  try {
    val ftp: FTPClient = new FTPClient()
    ftp.connect(server)
    println(s"Reply String:\t${ftp.getReplyString}")
    println(s"Remote Port:\t${ftp.getRemotePort}")
    println(s"Remote Address:\t${ftp.getRemoteAddress}")

    if (!ftp.login(user, pass)) throw new Exception("Login failed")

    ftp.setFileType(FTP.BINARY_FILE_TYPE) // or FTP.ASCII_FILE_TYPE
    println(s"Status:\t${ftp.getStatus}")

    // Enter passive mode
    ftp.enterLocalPassiveMode()
    println("Passive mode on.")

    val filter = FTPFileFilters.NON_NULL
    val dirList: Array[FTPFile] = ftp.listFiles(inPath, filter)
    //    val yearPattern = "^[0-9]{4}.csv.gz$".r
    //    val yearPattern = "^176[4,5].csv.gz$".r
    val yearPattern = "^190[1-9]{1}.csv.gz$|191[0-9]{1}.csv.gz$|^192[0-9]{1}.csv.gz$|^1930.csv.gz$".r

    for (d <- dirList) {
      yearPattern.findFirstIn(d.getName) match {
        case Some(s) => by_year(server, inPath, s, outPath)
        case None =>
      } // match
    } // for
    ftp.logout
    ftp.disconnect
    println(s"${dirList.length} file entries in remote directory")
    /* only download files w/ ftp ...
        val chooser = new FileChooser
        chooser.multiSelectionEnabled = true
        chooser.title = "Open Year Files"
        if (chooser.showOpenDialog(null) == FileChooser.Result.Approve) {
          val n = chooser.selectedFiles.size
          println(s"$n files selected")
          val fileName = chooser.selectedFiles(0).getName
          println(s"1: $fileName")
          //        val df1 = createRDD(spark, outPath, selectedFile.get(0).getName)//.as[GHCNData]
          val rdd1 = createRDD(outPath, fileName)
          //        println(rdd1.count())

          var rdd = rdd1
          //        println(s"dataframe 1 created w/ ${rdd1.count()} records")
          if (n > 1) {
            for (i <- 1 until n) {
              println(s"${i + 1}: ${chooser.selectedFiles(i).getName}")
              //            val df2 = createRDD(spark, outPath, selectedFile.get(i).getName)
              val rdd2 = createRDD(outPath, chooser.selectedFiles(i).getName)
              //            println(s"dataframe $i created w/ ${rdd2.count()} records")
              rdd = rdd.union(rdd2)
              println("create union of dataframes")
            } // for
          } // if
          //        println(s"union created w/ ${rdd.count()} records")

          // from rdd to dataframe
          val sqlContext = spark.sqlContext
          import sqlContext.implicits._
          val df = rdd.toDF()

          // from dataframe to dataset
          val ds = df
            .withColumnRenamed("id", "sid")
            .withColumnRenamed("element", "obs")
            .as[GHCNData]

          val parquetFile = outPath + "CSN###1" + ".parquet"
          df.write.mode(SaveMode.Overwrite).parquet(parquetFile)
          println("parquet file saved")
          val newDF = sqlContext.read.parquet(parquetFile)
          newDF.show(11)
          println(f"of ${newDF.count()}%,d records in parquet file $parquetFile")
        } // if
    */
  } catch {
    case e: UnsupportedOperationException =>
      println("Handler for ...")
      e.printStackTrace()
    case e: IOException =>
      println("Handler for IO Exception...")
      e.printStackTrace()
    case e: AssertionError =>
      println("Error handler for AssertionError ???")
      e.printStackTrace()
    case e: ArrayIndexOutOfBoundsException =>
      println("Error handler for ArrayIndexOutOfBoundsException ???")
      e.printStackTrace()
    case _: Throwable =>
      println("Got some other kind of exception")
  }
  finally {

    spark.stop()
  }


}
