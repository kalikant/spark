/*
covert json file into a dataframe to query it like sql.
*/
import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;


public class JSONFileToDataFrame.java 
{
    public static void main( String[] args )
    {
    	 final SparkConf sparkConf = new
    			 SparkConf()
    			 .setAppName("HiveExample")
    			 .setMaster("local");
    			
    			SparkContext sc=new SparkContext(sparkConf);
    			JavaSparkContext jsc = new JavaSparkContext(sc);
    			SQLContext sqlContext = new SQLContext(jsc);
    			DataFrame df = sqlContext.read().json("C:\\workspace\\employee.json");
    			System.out.println("printing");
    			df.show();
    			df.printSchema();
    			df.select("name").show();
    			df.select(df.col("name"), df.col("age").plus(1)).show();
    			df.filter(df.col("age").gt(21)).show();
    			df.groupBy("age").count().show();
    			
    }
    
    public static class Person implements Serializable {
    
		private static final long serialVersionUID = 1L;
		private String name;
    	  private int age;

    	  public String getName() {
    	    return name;
    	  }

    	  public void setName(String name) {
    	    this.name = name;
    	  }

    	  public int getAge() {
    	    return age;
    	  }

    	  public void setAge(int age) {
    	    this.age = age;
    	  }
    	}
}
