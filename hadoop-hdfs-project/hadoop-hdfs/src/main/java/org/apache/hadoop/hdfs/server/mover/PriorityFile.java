package org.apache.hadoop.hdfs.server.mover;

import org.apache.hadoop.fs.Path;

public class PriorityFile {
	private String path;
	private int w_count;
	private int d_count;
	private int m_count;
	
	public PriorityFile() {
	}

	public PriorityFile(String p) {
		this.path=p;
		this.w_count=0;
		this.d_count=0;
		this.m_count=0;
		
	}
	public PriorityFile(String p, int pri,int d,int w,int m) {
		this.path=p;
		this.w_count=w;
		this.d_count=d;
		this.m_count=m;
	}
	public String getPath()
	{
		return path;
	}
	public void setPath(String p)
	{
		this.path=p;
	}
	public int getW_count()
	{
		return w_count;
	}
	public void setW_count(int c)
	{
		this.w_count=c;
	}
	public int getM_count()
	{
		return m_count;
	}
	public void setM_count(int c)
	{
		this.m_count=c;
	}
	public int getD_count()
	{
		return d_count;
	}
	public void setD_count(int c)
	{
		this.d_count=c;
	}
}
