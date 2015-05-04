package org.apache.hadoop.hdfs.server.mover;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.mover.PriorityFile;
public class PolicySetter extends Configured{
	
	List<PriorityFile> FileList;
	long checkpointTime;
	private DistributedFileSystem getDFS() throws IOException {
		    FileSystem fs = FileSystem.get(new Configuration());
		    if (!(fs instanceof DistributedFileSystem)) {
		      throw new IllegalArgumentException("FileSystem " + fs.getUri() + 
		      " is not an HDFS file system");
		    }
		    return (DistributedFileSystem)fs;
		  }
	private void getFiles(DistributedFileSystem dfs) throws FileNotFoundException, IllegalArgumentException, IOException
	{
		if(dfs==null)
			throw new IllegalArgumentException("dfs now found"+this.getClass().getName());
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
	public static void main(String[] argsv) throws IOException
	{	PolicySetter deamon=new PolicySetter();
		DistributedFileSystem dfs;
		try {
			dfs = deamon.getDFS();
		} catch (IOException e) {
			System.out.println("unale to get dfs in"+e.getClass().getName());
			throw e;
		}
		try {
			deamon.getFiles(dfs);
		} catch (FileNotFoundException e) {
			System.out.println("Unable to scan DFS from '/'");
			throw e;
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		}
		
	}
}




