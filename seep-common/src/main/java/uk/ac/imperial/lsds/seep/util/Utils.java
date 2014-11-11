package uk.ac.imperial.lsds.seep.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

	final static private Logger LOG = LoggerFactory.getLogger(Utils.class);
	
	public static String NL = System.getProperty("line.separator");
	
	public static int computeIdFromIpAndPort(InetAddress ip, int port){
		return ip.hashCode() + port;
	}
	
	public static File writeDataToFile(byte[] serializedFile, String fileName){
		FileOutputStream fos;
		try {
			fos = new FileOutputStream(new File(fileName));
			fos.write(serializedFile);
			fos.close();
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		//At this point we should have the file on disk
		File pathToCode = new File(fileName);
		if(!pathToCode.exists()){
			return null;
		}
		return pathToCode;
	}
	
	public static byte[] readDataFromFile(String path){
		FileInputStream fis = null;
		long fileSize = 0;
		byte[] data = null;
		try {
			//Open stream to file
			LOG.debug("Opening stream to file: {}", path);
			File f = new File(path);
			fis = new FileInputStream(f);
			fileSize = f.length();
			//Read file data
			data = new byte[(int)fileSize];
			int readBytesFromFile = fis.read(data);
			//Check if we have read correctly
			if(readBytesFromFile != fileSize){
				LOG.warn("Mismatch between read bytes and file size");
				fis.close();
				return null;
			}
			//Close the stream
			fis.close();
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		finally{
			try {
				fis.close();
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		return data;
	}
	
	public static Properties readPropertiesFromFile(String fileName, boolean isResource){
		Properties prop = new Properties();
		try {
			InputStream fis = null;
			if(isResource)
				fis = (InputStream) Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
			else
				fis = new FileInputStream(new File(fileName));
			if(fis != null)
				prop.load(fis);
		}
		catch (FileNotFoundException e1) {
			System.out.println("Properties file not found "+e1.getMessage());
			e1.printStackTrace();
		}
		catch (IOException e1) {
			System.out.println("While loading properties file "+e1.getMessage());
			e1.printStackTrace();
		}
		return prop;
	}
	
	public static Properties overwriteSecondPropertiesWithFirst(Properties commandLineProperties, Properties fileProperties) {
		for(Object key : commandLineProperties.keySet()){
			fileProperties.put(key, commandLineProperties.get(key));
		}
		return fileProperties;
	}
	
	public static String getStringRepresentationOfLocalIp(){
		String ipStr = null;
		try {
			InetAddress myIp = InetAddress.getLocalHost();
			ipStr = myIp.getHostAddress();
		} 
		catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return ipStr;
	}
	
	public static InetAddress getLocalIp(){
		InetAddress myIp = null;
		try {
			myIp = InetAddress.getLocalHost();
		} 
		catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return myIp;
	}
	
}
