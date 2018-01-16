/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import scala.collection.mutable.{ArrayBuffer, HashMap}

import sys.process._

import org.apache.spark.sql.SparkSession

object BenchmarkTest {
  def main(args: Array[String]) {
    if (args.length < 6) {
      sys.error("Please config the arguments for testing!")
    }
    // e.g., 2 for 0.2.0
    val versionNum = args(0)

    // "oap" or "parquet" or "both"
    val dataFormats: Seq[String] = args(1) match {
      case "both" => Seq("oap", "parquet")
      case "oap" | "parquet" => Seq(args(1))
      case _ => Seq()
    }

    // data scale normally varies among [1, 1000]
    val dataScale = args(2)

    // 3=11 for all tests, 1=01 for baseline tests, 2=10 for indexes tests
    val useIndexes: Seq[Boolean] = args(3).toInt match {
      case 3 => Seq(true, false)
      case 2 => Seq(true)
      case 1 => Seq(false)
      case _ => Seq()
    }

    // 7=111 to test all indexes, 5=101 to test B-tree and trie, 6=110 to test B-tree and Bitmap
    val indexFlags = args(4).toInt
    val testTimes = args(5).toInt

    if(testTimes < 1) sys.error("Test times should be positive!")

    Array(1, 2, 4).foreach(indexMask => {
      if ((indexMask & indexFlags) > 0) {
        println(indexMask match {
          case 1 => "Test results of Trie index:"
          case 2 => "Test results of Bitmap index:"
          case 4 => "Test results of Btree index:"
        })
        val resMap = HashMap[String, Seq[ArrayBuffer[Int]]]()
        var queryNums = 0
        dataFormats.foreach(dataFormat => {
          useIndexes.foreach(useIndex => {
            implicit val spark = SparkSession.builder.appName(s"OAP-Test-${versionNum}.0")
              .enableHiveSupport().getOrCreate()
            spark.sqlContext.setConf("spark.sql.oap.oindex.enabled", s"${useIndex}")
            spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "gzip")
            spark.sql(s"USE ${dataFormat}tpcds${dataScale}")
            resMap.put(s"${dataFormat}-${useIndex}", indexMask match {
              case 1 => (1 to testTimes).map(_ => testForTrieIndex(useIndex))
              case 2 => (1 to testTimes).map(_ => testForBitmapIndex(useIndex))
              case _ => (1 to testTimes).map(_ => testForBtreeIndex(useIndex))
            })
            queryNums = resMap.get(s"${dataFormat}-${useIndex}").get(0).size
            spark.stop()
            assert(("rm -f ./metastore_db/db.lck" !) == 0)
            assert(("rm -f ./metastore_db/dbex.lck" !) == 0)
            assert(("sync" !) == 0)
            var dropCacheCmd: Seq[String] = Seq("bash", "-c", "echo 3 > /proc/sys/vm/drop_caches")
            assert(dropCacheCmd.! == 0)
          })
        })
        val res = dataFormats.flatMap(dataFormat =>
          useIndexes.map(useIndex =>
            (s"${dataFormat}-${if (useIndex) "with-index" else "without-index"}",
              resMap.get(s"${dataFormat}-${useIndex}").get)
          )
        )
        for(i <- 1 to queryNums) {
          val header = Seq(s"Q${i}") ++ (1 to testTimes).map("T" + _ +"/ms") ++ Seq("Avg/ms")
          val content = res.map(x =>
            Seq(x._1) ++ x._2.map(_(i - 1)) ++ Seq(x._2.map(_(i - 1)).sum / testTimes)
          )
          println(Tabulator.format(Seq(header) ++ content))
        }
      }
    })

    def testForBtreeIndex(useIndex: Boolean)(implicit spark: SparkSession) : ArrayBuffer[Int] = {
      // B-tree index on $d table store_sales (ss_ticket_number)
      val table = "store_sales"
      val attr = "ss_ticket_number"
      implicit val resList = ArrayBuffer[Int]()
      // Single column query
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr < 2000000").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr < 100000").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr < 10000").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr = 6000000").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr BETWEEN 100 AND 200")
        .foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr BETWEEN 100 AND 400")
        .foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr BETWEEN 100 AND 800")
        .foreach{ _ => })
      // Two columns query
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr < 2000000" +
        s" AND ss_customer_sk >= 120000").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr < 100000 " +
        s"AND ss_list_price < 100.0").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr < 10000 " +
        s"AND ss_net_paid > 100.0 AND ss_net_paid < 200.0").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr < 10000 " +
        s"AND ss_net_paid BETWEEN 100.0 AND 110.0").foreach{ _ => })
      // Three columns query
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr < 2000000 AND" +
        s" ss_customer_sk >= 120000 AND ss_list_price < 100.0").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr < 100000 AND " +
        s"ss_list_price < 100.0 AND ss_net_paid > 500.0").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr < 10000 AND " +
        s"ss_net_paid > 100.0 AND ss_net_paid < 110.0 AND ss_list_price < 100.0").foreach{ _ => })
      resList
    }
    def testForTrieIndex(useIndex: Boolean)(implicit spark: SparkSession) : ArrayBuffer[Int] = {
      val table = "customer"
      val attr = "c_email_address"
      implicit val resList = ArrayBuffer[Int]()
      // Single column query
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr LIKE '%.org'")
        .foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr LIKE 'P%.edu'")
        .foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr LIKE 'Irene%'")
        .foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr = " +
        s"'Charles.Swanson@SuOeUpKkthHS3.org'").foreach{ _ => })

      // Two columns query
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr LIKE '%.org'" +
        s" AND c_birth_year > 1978").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr LIKE 'P%.edu' " +
        s"AND c_birth_month > 4").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr LIKE 'Irene%' " +
        s"AND c_birth_country = 'BRAZIL'").foreach{ _ => })

      // Three columns query
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr LIKE '%.org' " +
        s"AND c_birth_year > 1978 AND c_birth_day BETWEEN 4 AND 10").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr LIKE 'P%.edu' " +
        s"AND c_birth_month > 4 AND c_birth_country = 'BRAZIL'").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr LIKE 'Irene%' " +
        s"AND c_birth_country = 'MEXICO' AND c_birth_year > 1978").foreach{ _ => })
      resList
    }

    def testForBitmapIndex(useIndex: Boolean)(implicit spark: SparkSession) : ArrayBuffer[Int] = {
      // Bitmap index on $d table store_sales (ss_item_sk)
      val table = "store_sales"
      val attr = "ss_item_sk1"
      implicit val resList = ArrayBuffer[Int]()
      val lsRange = (1 to 10).mkString(",")
      val msRange = (1 to 5).mkString(",")

      // Single column query
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr in ( $lsRange )")
        .foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr in ( $msRange )")
        .foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr = 10").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr = 25").foreach{ _ => })

      // Two columns query
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr in ( $lsRange )" +
        s" AND ss_customer_sk >= 120000").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr in ( $msRange ) " +
        s"AND ss_list_price < 100.0").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr = 10 " +
        s"AND ss_net_paid > 100.0 AND ss_net_paid < 200.0").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr = 25 " +
        s"AND ss_net_paid > 100.0 AND ss_net_paid < 200.0").foreach{ _ => })

      // Three columns query
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr in ( $lsRange ) " +
        s"AND ss_customer_sk >= 120000 AND ss_list_price < 100.0").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr in ( $msRange ) " +
        s"AND ss_list_price < 100.0 AND ss_net_paid > 500.0").foreach{ _ => })
      TestUtil.queryTime(spark.sql(s"SELECT * FROM $table WHERE $attr = 10 AND " +
        s"ss_net_paid > 100.0 AND ss_net_paid < 200.0 AND ss_list_price < 100.0").foreach{ _ => })
      resList
    }

  }
}
