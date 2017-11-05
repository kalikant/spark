/*
it is helper class to replace ctrl-a character in spark
*/

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object SparkSed {
  
   def main(args: Array[String]) {
    
    val sparkConf = new SparkConf().setAppName("Sed Replace").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile("inbox/test.dat")
    
    var finalLine=lines.map ({ x => x.replace(String.valueOf(0x00A7),String.valueOf(0x001)) });
    finalLine.collect().foreach { println }
   }
     
   println(System.getProperty("file.encoding"));
   println(0x00A7.toChar)
   println(0x001.toChar)
}
