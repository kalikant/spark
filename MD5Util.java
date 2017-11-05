/*
* this file generates checksum value of a HDFS file which match with UNIX checksum
* to use this Util class, need to pass FileSystem instance to it.
*
*/

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MD5Util {

	static MessageDigest complete =null;
	
	public static byte[] createChecksum(FileSystem fileSystem,String filePath) {
		
		FSDataInputStream  input = null;
		
		try {
			complete = MessageDigest.getInstance("MD5");
			Configuration hdfsConf = new Configuration();
			fileSystem.get(hdfsConf);
			input = fileSystem.open(new Path(filePath));
			byte[] buffer = new byte[1024];
			   
			    int numRead;

			    do {
			        numRead = input.read(buffer);
			        if (numRead > 0) {
			            complete.update(buffer, 0, numRead);
			        }
			    } while (numRead != -1);
			
		}catch (NoSuchAlgorithmException ex) {
			ex.printStackTrace(); 
		}
		catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	   

	    return complete.digest();
	}

	// a byte array to a HEX string
	public static String getMD5Checksum(FileSystem fileSystem,String filename) {
	    byte[] b = createChecksum(fileSystem,filename);
	    String result = "";

	    for (int i = 0; i < b.length; i++) {
	        result += Integer.toString((b[i] & 0xff) + 0x100, 16).substring(1);
	    }
	    return result;
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
