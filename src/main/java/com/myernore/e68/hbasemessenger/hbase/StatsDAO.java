package com.myernore.e68.hbasemessenger.hbase;

import org.apache.hadoop.hbase.util.Bytes;

public class StatsDAO {
	public static final byte[] TABLE_NAME = Bytes.toBytes("stats");
	public static final byte[] INFO_FAM = Bytes.toBytes("inf");

	// Info Column Family Columns
	public static final byte[] NUM_MESSAGES = null;
	public static final byte[] NUM_USERS = null;
	
	
}
