import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

public class SparkJavaPairRDD {

	final static SparkConf sparkConf = new
  			 SparkConf()
  			 .setAppName("JavaPiarRDD")
  			 .setMaster("local");
	
  		
			static SparkContext sc=new SparkContext(sparkConf);
  			static JavaSparkContext jsc = new JavaSparkContext(sc);
  			static SQLContext sqlContext = new SQLContext(jsc);
  			
  			static long rownum=0L;
  			
	public static void main(String[] args) {
		
   			
		run(args[0]);

	}
	
	public static void run(String fileList) {
		
		JavaPairRDD<String, String> filesData =null;
		Map<String,JavaPairRDD<String, String>> dataMap=new HashMap<>();
		
		for(String file:fileList.split(","))
		{
			JavaRDD<String> eachFile = jsc.textFile(file);
			
			filesData = eachFile
					.mapToPair(new PairFunction<String, String, String>() {
						public Tuple2<String, String> call(String s) {
							++rownum;
							String key= jsc.getConf().getAppId() + rownum +"," ;
							String value =key + s; 
							return new Tuple2<String, String>(file,value);
						}
					});
			dataMap.put(file, filesData);
			
		}
		
				
		Iterator<Map.Entry<String,JavaPairRDD<String, String>>> fileIterator = dataMap.entrySet().iterator();
		while(fileIterator.hasNext())
		{
			Map.Entry<String,JavaPairRDD<String, String>> entry = fileIterator.next();
			filesData=entry.getValue();
			Iterator dataIterator=filesData.collect().iterator();
			while (dataIterator.hasNext())
			System.out.println(dataIterator.next());
		}
		
				
	}



}
