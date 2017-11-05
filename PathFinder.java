import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

public class PathFinder {
	private static final Logger logger = Logger.getLogger(PathFinder.class);

	public enum MatchType {
		EQUALS, STARTSWITH, ENDSWITH
	};

	private Configuration configuration;
	private FileSystem fileSystem;
	private String parentPath;
	private Path path;
	private String tableName = null;
	private String tableSourceType = null;
	private MatchType tableMatchType = MatchType.EQUALS;
	private String partition = null;
	private String fileName = null;

	public PathFinder(Configuration conf, String parentPath) throws IOException {
		this.configuration = conf;
		this.fileSystem = FileSystem.get(configuration);
		this.parentPath = parentPath;
		this.path = new Path(parentPath);
	}

	public String getParentPath() {
		return parentPath;
	}

	public void setParentPath(String parentPath) {
		this.parentPath = parentPath;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getTableSourceType() {
		return tableSourceType;
	}

	public void setTableSourceType(String tableSourceType) {
		this.tableSourceType = tableSourceType;
	}

	public MatchType getTableMatchType() {
		return tableMatchType;
	}

	public void setTableMatchType(MatchType tableMatchType) {
		this.tableMatchType = tableMatchType;
	}

	public String getPartition() {
		return partition;
	}

	public void setPartition(String partition) {
		this.partition = partition;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public List<Path> getFiles() throws IOException {
		try {
			// Retrieve tables
			List<Path> tableList = getPaths(path, tableName, tableMatchType);
			// Filter out tables by source type
			if (tableSourceType != null) {
				Iterator<Path> it = tableList.iterator();
				while (it.hasNext()) {
					String sourceType = configuration
							.get("table."
									+ it.next().getName() + ".sourcetype");
					if (!tableSourceType.equalsIgnoreCase(sourceType)) {
						it.remove();
					}
				}
			}
			if (partition == null) {
				logger.info(this.toString() + ": " + tableList.size()
						+ " files found.");
				return tableList;
			}
			// Retrieve partitions
			List<Path> partitionList = new ArrayList<Path>();
			for (Path table : tableList) {
				partitionList.addAll(getPaths(table, partition,
						MatchType.ENDSWITH));
			}
			if (fileName == null) {
				logger.info(this.toString() + ": " + partitionList.size()
						+ " files found.");
				return partitionList;
			}
			// Retrieve files
			List<Path> outputFiles = new ArrayList<Path>();
			for (Path part : partitionList) {
				outputFiles.addAll(getPaths(part, fileName,
						MatchType.STARTSWITH));
			}
			logger.info(this.toString() + ": " + outputFiles.size()
					+ " files found.");
			return outputFiles;
		} catch (IOException e) {
			logger.error(
					"IOException retrieving files in finder " + this.toString(),
					e);
			throw e;
		}
	}

	private List<Path> getPaths(Path parentPath, final String name,
			final MatchType matchType) throws IOException {
		List<Path> outputList = new ArrayList<Path>();
		try {
			FileStatus[] fileList = fileSystem.listStatus(parentPath,
					new PathFilter() {
						@Override
						public boolean accept(Path path) {
							if (name == null || name.trim().isEmpty()) {
								return true;
							}
							switch (matchType) {
							case ENDSWITH:
								return path.getName().endsWith(name);
							case STARTSWITH:
								return path.getName().startsWith(name);
							case EQUALS:
							default:
								return path.getName().equals(name);
							}

						}
					});
			if (fileList == null) {
				fileList = new FileStatus[] {};
			}
			for (FileStatus fs : fileList) {
				outputList.add(fs.getPath());
			}
			return outputList;
		} catch (FileNotFoundException e) {
			logger.warn("Ignoring path " + parentPath + ": " + e.getMessage());
		}
		return outputList;
	}

	@Override
	public String toString() {
		return "PathFinder [path=" + path + ", tableName=" + tableName
				+ ", tableSourceType=" + tableSourceType + ", tableMatchType="
				+ tableMatchType + ", partition=" + partition + ", fileName="
				+ fileName + "]";
	}
}
