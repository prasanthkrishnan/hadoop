package org.apache.hadoop.hdfs.server.mover;

import java.io.*;
import java.util.*;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.mover.PriorityFile;

public class PolicySetter extends Configured{

	List<PriorityFile> fileList;
	long checkpointTime;
	private static final String OUTPUTFILENAME = "mover_input.txt";
	private PrintWriter out = null;
	private DistributedFileSystem dfs = null;

	public PolicySetter(){
		try
		{
			File outputFile = new File(OUTPUTFILENAME);
			if(outputFile.exists() && !outputFile.isDirectory()) {
				outputFile.delete();	
			}
			outputFile.createNewFile();
			dfs = getDFS();
		}
		catch(IOException ioe)
		{
			System.out.println("Error while creating a new empty file in PolicySetter");
		}
	}

	private DistributedFileSystem getDFS() throws IOException {
		FileSystem fs = FileSystem.get(getConf());
		if (!(fs instanceof DistributedFileSystem)) {
			throw new IllegalArgumentException("FileSystem " + fs.getUri() + " is not an HDFS file system");
		}
		return (DistributedFileSystem)fs;
	}

	private void getFiles() throws FileNotFoundException, IllegalArgumentException, IOException
	{
		if(dfs==null){
			throw new IllegalArgumentException("dfs now found"+this.getClass().getName());
		}
		RemoteIterator<LocatedFileStatus> ri=dfs.listFiles(new Path("/"), true);
		while(ri.hasNext())
		{
			LocatedFileStatus status=ri.next();
			System.out.println("---------------FILE--------------");
			System.out.println(status.getPath());
			System.out.println("is dir: "+status.isDirectory());
			System.out.println("is file : "+status.isFile());
			System.out.println("is symlink : "+status.isSymlink());
			System.out.println("block size : "+status.getBlockSize());
			System.out.println("length : "+status.getLen());
			System.out.println("---------------FILE--------------");
		}
	}

	public void setPolicy(){
		for(PriorityFile priorityFile: fileList){
			int m = priorityFile.getM_count();
			int w = priorityFile.getW_count();
			int d = priorityFile.getD_count();
			byte targetStoragePolicy = HdfsConstants.HOT_STORAGE_POLICY_ID;
			if(m == 0){
				targetStoragePolicy = HdfsConstants.COLD_STORAGE_POLICY_ID;	
			}
			else if(m > 0 && w == 0){
				targetStoragePolicy = HdfsConstants.WARM_STORAGE_POLICY_ID;	
			}
			else if(d > 2 && w > 7){
				targetStoragePolicy = HdfsConstants.ONESSD_STORAGE_POLICY_ID;	
			}
			else if(d >10){
				targetStoragePolicy = HdfsConstants.ALLSSD_STORAGE_POLICY_ID;	
			}
			byte currentStoragePolicy = (byte)0;	
			try{	
				currentStoragePolicy = getStoragePolicy(priorityFile.getPath());
			}
			catch(Exception e){
				System.out.println("Caught unknown exception, continuing!!!");
				continue;
			}
			if(currentStoragePolicy == (byte)0){
				continue;
			}
			if(currentStoragePolicy != targetStoragePolicy){
				changePolicy(priorityFile.getPath(), targetStoragePolicy);
			} 
		}
	}

	private void changePolicy(String path, byte targetStoragePolicy){
		setStoragePolicy(path, byteToStringStoragePolicy(targetStoragePolicy));						
		writePathToFile(path);
	}

	private void writePathToFile(String path){
		try {
			out = new PrintWriter(new BufferedWriter(new FileWriter(OUTPUTFILENAME, true)));
			out.print(path + ",");
			out.close();
		} catch (IOException e) {
			System.out.println("Error while wrriiiting to file");
			return;
		}
	}

	String byteToStringStoragePolicy(byte byteStoragePolicy){
		if(byteStoragePolicy == HdfsConstants.COLD_STORAGE_POLICY_ID){
			return HdfsConstants.COLD_STORAGE_POLICY_NAME;
		}
		else if(byteStoragePolicy == HdfsConstants.WARM_STORAGE_POLICY_ID){
			return HdfsConstants.WARM_STORAGE_POLICY_NAME;
		}
		else if(byteStoragePolicy == HdfsConstants.HOT_STORAGE_POLICY_ID){
			return HdfsConstants.HOT_STORAGE_POLICY_NAME;
		}
		else if(byteStoragePolicy == HdfsConstants.ONESSD_STORAGE_POLICY_ID){
			return HdfsConstants.ONESSD_STORAGE_POLICY_NAME;
		}
		else if(byteStoragePolicy == HdfsConstants.ALLSSD_STORAGE_POLICY_ID){
			return HdfsConstants.ALLSSD_STORAGE_POLICY_NAME;
		}
		else {
			throw new RuntimeException("Cannot find appropriate Storage policy for id !!!!!! OH MY GOD!!");
		}
	}

	public void setStoragePolicy(String path, String policy) throws IOException {
		dfs.setStoragePolicy(new Path(path), policy);
		System.out.println("Set storage policy " + policy + " on " + path);
		return;
	}

	public byte getStoragePolicy(String argv) throws IOException {
		HdfsFileStatus status = dfs.getClient().getFileInfo(argv);
		if (status == null) {
			throw new FileNotFoundException("File/Directory does not exist: " + argv);
		}
		byte storagePolicyId = status.getStoragePolicy();
		if (storagePolicyId == BlockStoragePolicySuite.ID_UNSPECIFIED) {
			System.out.println("The storage policy of " + argv + " is unspecified");
			return (byte)0;
		}
		else{
			return storagePolicyId;
		}
	}


	public static void main(String[] args) throws IOException
	{	PolicySetter deamon=new PolicySetter();
		try {
			deamon.getFiles();
		} catch (FileNotFoundException e) {
			System.out.println("Unable to scan DFS from '/'");
			throw e;
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		}
		try {
			deamon.setPolicy();
		} catch (IOException e) {
			System.out.println("unale to set dfs in......");
			throw e;
		}
	}
}

