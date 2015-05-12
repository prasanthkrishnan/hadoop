package org.apache.hadoop.hdfs.server.mover;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.mover.PriorityFile;
import org.apache.hadoop.hdfs.server.mover.TimestampUtils;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.server.mover.Mover;

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
			checkpointTime=System.currentTimeMillis()/1000L;
			fileList = new ArrayList<PriorityFile>();
		}
		catch(IOException ioe)
		{
			System.out.println("Error while creating a new empty file in PolicySetter");
		}
	}

	private DistributedFileSystem getDFS() throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		if (!(fs instanceof DistributedFileSystem)) {
			throw new IllegalArgumentException("FileSystem " + fs.getUri() + " is not an HDFS file system");
		}
		System.out.println("System URI:" + fs.getUri());
		return (DistributedFileSystem)fs;
	}

	private void getFiles() throws FileNotFoundException, IllegalArgumentException, IOException
	{
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
			Path currentFile=status.getPath();
			PriorityFile currentFilePriority= new PriorityFile(currentFile);
			byte[] atime=dfs.getXAttr(currentFile, DFSClient.ACCESSTIMES);
			if(atime==null){
				continue;
			}
			System.out.println("atime " + atime.toString());
			String atimeString = new String(atime);
			List<java.sql.Timestamp>atimeList=TimestampUtils.convertToTimeStamps(atimeString);
			System.out.println("Timelist Size " + atimeList.size());
			for (Timestamp timestamp : atimeList) {
				System.out.println("checkpointTime " + checkpointTime);
				if(TimestampUtils.isWithinDay(timestamp, checkpointTime))
				{
					System.out.println("IsWithinDay");
					currentFilePriority.setD_count((currentFilePriority.getD_count()+1));
					currentFilePriority.setW_count((currentFilePriority.getW_count()+1));
					currentFilePriority.setM_count((currentFilePriority.getM_count()+1));
					continue;
				}
				if(TimestampUtils.isWithinWeek(timestamp, checkpointTime))
				{
					System.out.println("IsWithinWeek");
					currentFilePriority.setW_count((currentFilePriority.getW_count()+1));
					currentFilePriority.setM_count((currentFilePriority.getM_count()+1));
					continue;
				}
				if(TimestampUtils.isWithinMonth(timestamp, checkpointTime)){
					System.out.println("IsWithinMonth");
					currentFilePriority.setM_count((currentFilePriority.getM_count()+1));
					continue;
				}
				System.out.println("Not withinnnnnnn anything" + timestamp.toString());
			}
			//if(currentFilePriority.getD_count()!=0 && currentFilePriority.getM_count()!=0 && currentFilePriority.getW_count()!=0){
			fileList.add(currentFilePriority);
			System.out.println("filelist size" + fileList.size());
				//}
		}
	}
	public void setPolicy(){
		int filesPolicyChanged = 0;
		for(PriorityFile priorityFile: fileList){
			int m = priorityFile.getM_count();
			int w = priorityFile.getW_count();
			int d = priorityFile.getD_count();
			System.out.println("days " + d);
			System.out.println("weeks " + w);
			System.out.println("months " + m);
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
			System.out.println("targetStorgePolicy " + targetStoragePolicy);
			byte currentStoragePolicy = (byte)0;	
			try{	
				currentStoragePolicy = getStoragePolicy(priorityFile.getPath().toString());
			}
			catch(Exception e){
				e.printStackTrace();
				System.out.println("Caught unknown exception, continuing!!!");
				continue;
			}
			System.out.println("currentStorgePolicy " + currentStoragePolicy);
			/*if(currentStoragePolicy == (byte)0){
				continue;
			}*/
			if(currentStoragePolicy != targetStoragePolicy){
				System.out.println("current policy not equals target policy ");
				filesPolicyChanged++;
				try {
					changePolicy(priorityFile.getPath().toString(), targetStoragePolicy);
				} catch (IOException e) {
					e.printStackTrace();
					continue;
				}
			} 
		}
		System.out.println("No of files with Policy changed: " + filesPolicyChanged);
		if(filesPolicyChanged != 0){
			//Make call to mover
			String arg1 = "-f " + OUTPUTFILENAME;
			String[] args = {"", arg1};
			Mover.main(args);		
		}
	}

	private void changePolicy(String path, byte targetStoragePolicy) throws IOException{
		setStoragePolicy(path, byteToStringStoragePolicy(targetStoragePolicy));						
		System.out.println("wrriiiting to file");
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
	
	public static void main(String[] args) throws IOException{	
		PolicySetter deamon=new PolicySetter();	
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
		deamon.setPolicy();
	}
}
