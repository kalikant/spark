/*
* This java class read a config directory and populate a Map and a Liat so that it can be used as KEY=VALUE 
* pair as we have property files in plian java. 
* 
* To use this class, need to pass javaSparkContext
*
*/

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;



public class ConfigLoader implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private static JavaSparkContext javaSparkContext=null;
	private static String configDir=null;
	private static Map<String,String[]> allConfigData=new HashMap<>();
	private static JavaRDD<Map<String,String[]>> confOutput =null;
	private static Map<String,String> propertyMap=new HashMap<>();
	private static List<String> tableList=new ArrayList<>();
	private static Map<String,Broadcast> broadcastValues=new HashMap<>();
	
	public ConfigLoader()
	{
		
	}
	
	public ConfigLoader(JavaSparkContext javaSparkContext,String configDir)
	{
		this.javaSparkContext=javaSparkContext;
		this.configDir=configDir;
	}
	
	
	 public static JavaRDD<Map<String,String[]>> readConfig() {
	        JavaPairRDD<String, String> fileNameContentsRDD = javaSparkContext.wholeTextFiles(configDir);
	        JavaRDD<Map<String,String[]>> lineContent = fileNameContentsRDD.map(new Function<Tuple2<String, String>, Map<String,String[]>>() {
	            @Override
	            public Map<String,String[]> call(Tuple2<String, String> fileNameContent) throws Exception {
	            	String fileName=fileNameContent._1();
	            	String fileData= fileNameContent._2();
	            	String[] rows=fileData.split("[\r\n]+");
	            	allConfigData.put(fileName, rows);
	            	return allConfigData;
	            }
	        });
	        return lineContent;
	        
//	        List<Map<String,String[]>> output = lineContent.collect();
//	        output.forEach(map->map.forEach((k,v)->{
//	        	for(String val:v)
//	        	{
//	        		System.out.println(k);
//		        	System.out.println(val);
//	        	}
//	        	
//	        }));
//	        
//	        
//	        for(Map<String,String[]> item : output){
//	        	 for (Map.Entry<String,String[]> entry : item.entrySet()) {
//	 	        	System.out.println("filename : " + entry.getKey());
//	 	        	System.out.println("value " + entry.getValue());
//	 	        }
//	        }
	        
	    }
	
	public static Map<String,Broadcast> getConfig()
	{
		confOutput=ConfigLoader.readConfig();
		List<Map<String,String[]>> output = confOutput.collect();
	        output.forEach(map->map.forEach((k,v)->{
	        	for(String val:v)
	        	{
	        		if(k.contains(".properties"))
	        		{
	        			System.out.println("key : "+k+" VAL :"+ val);
	        			String str[]=val.split("=");
	        			if(str.length == 2)
	        			{
		        			String key=str[0];
		        			String value=str[1];
		        			propertyMap.put(key,value);
	        			}
	        		}
	        		Broadcast<Map<String,String>> sharedPropertyMap=javaSparkContext.broadcast(propertyMap);
	        		broadcastValues.put("ext.config",sharedPropertyMap);
	        		System.out.println(sharedPropertyMap.value().get("hdfs.base.path"));
	        		if(k.contains("tables.list"))
	        			tableList.add(val);
	        		Broadcast<List<String>> sharedTableList=javaSparkContext.broadcast(tableList);
	        		broadcastValues.put("tables.list",sharedTableList);
	        		//System.out.println(sharedTableList.value().get(0));
	        		
	        	}
	        	
	        }));
	        return broadcastValues;
	}
	
	public void displsy()
	{
		System.out.println(propertyMap);
		System.out.println(tableList);
	}
	
	
}
