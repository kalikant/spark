/*
* This class has written to extract data from data lake which is in sequence file format
* it will extract in text format and also append header and trailer information to it
* calculate checksum value of final data file
* push back to UNIX server
*/

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import scala.Tuple2;

public class LakeDataExtract {

	// main arguments
	private static String extConfigPath;
	private static String tableListPath;
	private static String startDate;
	private static String endDate;
	private static String rowCount="";

	SparkConf conf = null;
	static JavaSparkContext jsc = null;
	HiveContext hiveContext = null;
	AuditData objAuditData = null;
	static FileSystem fileSystem = null;

	public static Map<String, String> extConfig = null;
	static final Logger logger = Logger.getLogger(LakeDataExtract.class);
	static List<String> tablesList = new ArrayList<>();
	static ConfigLoader configLoaderObj = null;
	static Map<String, Broadcast> broadcastValues = null;
	static boolean historicalLoad=false;

	LakeDataExtract() {
		// conf = new SparkConf().setMaster("local").setAppName("datalakeext");
		conf = new SparkConf();
		jsc = new JavaSparkContext(conf);
		conf.set("spark.hadoop.validateOutputSpecs", "false");
		hiveContext = new HiveContext(jsc);
		objAuditData = new AuditData();
		configLoaderObj = new ConfigLoader(jsc, tableListPath);
		broadcastValues = configLoaderObj.getConfig();
		Broadcast<Map<String, String>> sharedPropertyMap = broadcastValues.get("ext.config");
		extConfig = sharedPropertyMap.value();
		Broadcast<List<String>> sharedTableList = broadcastValues.get("tables.list");
		tablesList = sharedTableList.value();
		try {
			fileSystem = FileSystem.get(jsc.hadoopConfiguration());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static String getHeader()
	{
		String header="";
		String fileHeader=extConfig.get("file.header").toString();
		String country=extConfig.get("extraction.country").toString();
		String partitionDate="";
		if(historicalLoad)
			partitionDate=extConfig.get("partition.end.date").toString();
		else
			partitionDate=extConfig.get("partition.date").toString();
		
		header+=fileHeader+country+partitionDate+Util.getRunDate()+"1";
		return header;
	}
	
	public static String getTrailer()
	{
		return extConfig.get("file.trailer").toString();
	}

	public static void main(String[] args) {
		if (args.length == 2) {
			logger.info("It's incremental extraction for partition date " + args[1]);
			// extConfigPath = args[0];
			tableListPath = args[0];
			startDate = args[1];
			logger.info(" Job Execution Params " + extConfigPath + " " + tableListPath + " " + startDate);

			LakeDataExtract objLakeDataExtract = new LakeDataExtract();
			objLakeDataExtract.extManager();

		} else if (args.length == 3) {
			logger.info("It's historical extraction from  " + startDate + " to " + endDate + " ..");
			// extConfigPath = args[0];
			tableListPath = args[0];
			startDate = args[1];
			endDate = args[2];
			logger.info(
					" Job Execution Params " + extConfigPath + " " + tableListPath + " " + startDate + " " + endDate);

			LakeDataExtract objLakeDataExtract = new LakeDataExtract();
			objLakeDataExtract.extManager();

		} else {
			logger.info("Please provide ext.config,tables.list files and partition date ... ");
		}
	}

	private void configLocader() {

		// // loading properties
		// if (logger.isInfoEnabled())
		// logger.info("Loading extraction properties..");
		//
		// extConfig = new Properties();
		// extConfig.load(configLoaderObj.getConfig().get("ext.config").value().toString());
		//// FSDataInputStream input = null;
		// try {
		// String extConfigPath = SparkFiles.get("ext.properties");
		// Configuration hdfsConf = new Configuration();
		// fileSystem.get(hdfsConf);
		// input = fileSystem.open(new Path(extConfigPath));
		// extConfig.load(input);
		// logger.info(extConfig.getProperty("hdfs.base.path"));
		// System.out.println(extConfig.getProperty("hdfs.base.path"));
		//
		// } catch (IOException ex) {
		// ex.printStackTrace();
		// } finally {
		// if (input != null) {
		// try {
		// input.close();
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		// }
		// }

		// if (logger.isInfoEnabled())
		// logger.info("Application properties loaded..");
		//
		// // loading properties
		// if (logger.isInfoEnabled())
		// logger.info("Loading tables list..");
		//
		// JavaRDD<String> tableList=jsc.textFile(tableListPath);
		// tableList.collect().forEach((String line) -> {
		// tablesList.add(line);
		// System.out.println(line.toString());
		// logger.info(line.toString());
		//
		// });

	}

	private void extManager() {
		if ("Y".equalsIgnoreCase(extConfig.get("incremental.extract").toString())) {
			logger.info(" incremental.extract " + extConfig.get("incremental.extract").toString());
			incrementalExtract();
		}

		if ("Y".equalsIgnoreCase(extConfig.get("historical.extract").toString())) {
			logger.info(" historical.extract " + extConfig.get("historical.extract").toString());
			historicalLoad=true;
			historicalExtract();
		}

	}

	private void incrementalExtract() {
		if ("hdfs2hdfs".equalsIgnoreCase(extConfig.get("data.movement").toString())) {
			String sourcePath = extConfig.get("hdfs.source.path");
			String targetPath = extConfig.get("hdfs.target.path");
			sequence2text(sourcePath, targetPath);
		}

		if ("hive2hdfs".equalsIgnoreCase(extConfig.get("data.movement").toString())) {
			logger.info("hive2hdfs");
			for (String table : tablesList) {
				logger.info("Table list : " + table);
				sequence2text(table);
			}

		}
	}

	private void historicalExtract() {

	}

	private boolean isPartitionExisit(String path) {
		try {
			logger.info("path : " + path);
			return fileSystem.exists(new Path(path));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	private List<String> getDates() {
		String startDate = extConfig.get("partition.start.date").toString();
		String endDate = extConfig.get("partition.start.date").toString();

		return null;
	}

	private void queryExecuter(String query, String ouptputPath) {

		objAuditData.setDataFetchQuery(query);
		objAuditData.setDestinationPath(ouptputPath);
		DataFrame df = hiveContext.sql(query);
		RDD<Row> rows = df.rdd();
		HDFSUtil.deletePath(ouptputPath, jsc.hadoopConfiguration());
		rows.saveAsTextFile(ouptputPath);

		// Output the query's rows
		// df.javaRDD().collect().forEach((Row row) -> {
		// System.out.println(row.toString());
		// });

	}

	public static class ConvertToNativeTypes implements PairFunction<Tuple2<NullWritable, Text>, String, String> {

		private static final long serialVersionUID = 1L;

		public Tuple2<String, String> call(Tuple2<NullWritable, Text> record) {
			String key = record._1.toString();
			key = "D";
			String value = "D" + record._2.toString();
			// String value=record._2.toString().replaceAll("\u0001", "|");
			return new Tuple2(key, value);
		}
	}

	public static class getOnlyValue implements Function<Text, String> {

		private static final long serialVersionUID = 1L;

		public String call(Text record) {
			return "D"+record.toString();
		}
	}

	private void sequence2text(String table) {

		String sourcePath = extConfig.get("hdfs.base.path") + "/" + table + "/" + extConfig.get("partition.column")
				+ "=" + startDate + "/";
		String targetPath = extConfig.get("hdfs.target.path") + "/" + table + "/" + extConfig.get("partition.column")
		+ "=" + startDate + "/";
		String stagingPath= extConfig.get("hdfs.stg.path") + "/" + table + "/" + extConfig.get("partition.column")
				+ "=" + startDate + "/";
		logger.info("sourcePath : " + sourcePath);
		logger.info("targetPath : " + targetPath);

		if (isPartitionExisit(sourcePath)) {
			logger.info("inside sequence2text ... ");
			JavaPairRDD<NullWritable, Text> input = jsc.sequenceFile(sourcePath, NullWritable.class, Text.class);
			// JavaPairRDD<String, String> result = input.mapToPair(new ConvertToNativeTypes());
//			JavaPairRDD<NullWritable,String> result = input.mapValues(
//					  new Function<String,String>() {
//						    @Override
//						    public String call(String s) {
//						      return s;
//						    }					  }
//						  );
			JavaPairRDD<NullWritable,String> result = input.mapValues(new getOnlyValue());
			HDFSUtil.deletePath(stagingPath,jsc.hadoopConfiguration());
			result.coalesce(1).saveAsTextFile(targetPath);
			//result.saveAsTextFile(stagingPath);
			
			rowCount=Long.toString(result.count());
			String header=LakeDataExtract.getHeader();
			String trailer=LakeDataExtract.getTrailer()+rowCount;
			
			HDFSUtil.createHDFSFile(stagingPath, "1_header", header, jsc.hadoopConfiguration());
			HDFSUtil.createHDFSFile(stagingPath, "3_trailer", trailer, jsc.hadoopConfiguration());
			
			String mergedSrcPath = stagingPath;
			HDFSUtil.deletePath(mergedSrcPath ,jsc.hadoopConfiguration());
			String mergedDstPath = targetPath;
			HDFSUtil.mkdir(mergedDstPath,jsc.hadoopConfiguration());
			logger.info("SRC " + mergedSrcPath);
			logger.info("DST " + mergedDstPath);
			HDFSUtil.getHDFSMerge(mergedSrcPath, mergedDstPath, jsc.hadoopConfiguration());
			HDFSUtil.appendToFile(rowCount, mergedDstPath, jsc.hadoopConfiguration());
			// renaming target file
			String targetFileName=HDFSUtil.listHDFSFile(mergedDstPath,jsc.hadoopConfiguration());
			HDFSUtil.renameFile(targetFileName, table, jsc.hadoopConfiguration());
			// getting renamed file name
			targetFileName=HDFSUtil.listHDFSFile(mergedDstPath,jsc.hadoopConfiguration());
			
			String finalTargetFilePath=mergedDstPath+"/"+targetFileName;
			String fileHashValue=MD5Util.getMD5Checksum(fileSystem,finalTargetFilePath);
			String mainfestFile=extConfig.get("extraction.country") +"_"+ extConfig.get("source.system")+"_"+extConfig.get("partition.date")+".txt";
			String minfestFileContent=fileHashValue+" "+mainfestFile;
			HDFSUtil.createHDFSFile(mergedDstPath, mainfestFile, minfestFileContent, jsc.hadoopConfiguration());
			logger.info("MDF Checksum : " + fileHashValue);
			
			
		} else
			logger.info("hdfs partition is not available for given date ... ");

	}

	private void sequence2text(String sourcePath, String targetPath) {

		JavaPairRDD<NullWritable, Text> input = jsc.sequenceFile(sourcePath, NullWritable.class, Text.class);
		JavaPairRDD<String, String> result = input.mapToPair(new ConvertToNativeTypes());
		HDFSUtil.deletePath(targetPath, jsc.hadoopConfiguration());
		result.saveAsTextFile(targetPath);
		// List<Tuple2<String, Integer>> resultList = result.collect();
		// for (Tuple2<String, Integer> record : resultList) {
		// System.out.println(record);
		// }
	}

}
