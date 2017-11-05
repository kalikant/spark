/* 
this is elper class to remove double quote in data file
*/

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class DoubleQuote {

	public static void main(String args[]) {
		 final SparkConf sparkConf = new
		 SparkConf().setAppName("RemoveDoubleQuote").setMaster("local");
		//SparkConf sparkConf = new SparkConf().setAppName("RemoveDoubleQuote");
		SparkContext sc=new SparkContext(sparkConf);
		JavaSparkContext jSC = new JavaSparkContext(sc);
		
		if (null == args[0])
			System.out.println("Please provide input file path ..");
		else if (null == args[1])
			System.out.println("Please provide output file path ..");
		else if (null == args[2])
			System.out.println("Please provide column delimiter ..");
		else {

			JavaRDD<String> lines = jSC.textFile(args[0]);

			JavaRDD<String> cleanData = lines
					.flatMap(new FlatMapFunction<String, String>() {
						public Iterable<String> call(String s) {

							String[] strArr = s.replaceAll("\",\"", "|").replaceAll("\",+", "|").replaceAll("\",-", "|").replaceAll(",+", "|").replaceAll(",-", "|").split("\\|");
													
							String firstPart = strArr[0].replaceAll("\"", "")
									.replaceAll(",", "|") + "|";
							String secondPart = "";
							for (int i = 1; i < strArr.length; i++)
								secondPart +=  strArr[i] + "|";
							String finalLine = firstPart + secondPart;
							finalLine=finalLine.substring(0,finalLine.length()-1);
							return Arrays.asList(finalLine);
						}

					});

			 Iterator it = cleanData.collect().iterator();
			 while (it.hasNext())
			 System.out.println(it.next());

			//cleanData.saveAsTextFile(args[1]);

			System.out.println("Output file has written to .. " + args[1]);

		}

	}
}
