import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class SedReplace {
  
   def main(args: Array[String]) {
    
    val sparkConf = new SparkConf().setAppName("Sed Replace").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile("inbox/input.txt")

    var finalLines = lines.map(a => {
      var strArr = a.replaceAll("\\|", "\\/").replaceAll("\",\"", "|").split("\\|");
      var len = strArr.length;
      var newStrlist = List();
      var colValue="";
      for (i<- 1 to len -1)
      {
        var colValue=strArr(i)
        colValue.replaceAll(",+", "\\|").replaceAll(",-", "\\|")
          
      }
     
      var firstPart = strArr(0).replaceAll("\"", "").replaceAll(",", "|");
      var secondPart = "";
      for (i <- 1 to len - 1) {
        secondPart = secondPart + "|" + strArr(i);
      }
      var result = firstPart + secondPart
      if (result.charAt(result.length() - 1) == '\"') {
        result.substring(0, result.length() - 1)
      } else {

        result
      }

    })
    finalLines.foreach(println)

    //finalLines.saveAsTextFile(outputFile)

}
}
