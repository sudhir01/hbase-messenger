package com.myernore.e68.HbaseMessenger.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class UserConnection {
	
	private static final Logger log = Logger.getLogger(UserConnection.class);

	public static final byte[] TABLE_NAME = Bytes.toBytes("usr");
	public static final byte[] INFO_FAM = Bytes.toBytes("inf");
	public static final byte[] MSGS_FAM = Bytes.toBytes("msg");
	public static final byte[] MSGS_META_FAM = Bytes.toBytes("met");
	
	// Info Column Family Columns
	public static final byte[] USER_COL = null;
	public static final byte[] NAME_COL = null;
	
	// Messages Column Family Columns
	

	private HTablePool pool;

	public UserConnection(HTablePool pool) {
		this.pool = pool;
	}

	public void addUser(String username, String name) throws IOException {
		HTableInterface userTable = pool.getTable(TABLE_NAME);

		Put p = mkPut(new User(username, name));
		userTable.put(p);

		userTable.close();

	}
	
	private static Put mkPut(User u) {
	    log.debug(String.format("Creating Put for %s", u));

	    Put p = new Put(Bytes.toBytes(u.user));
	    p.add(INFO_FAM, USER_COL, Bytes.toBytes(u.user));
	    p.add(INFO_FAM, NAME_COL, Bytes.toBytes(u.name));
	    return p;
	  }
	
	private static Get mkGet(String user) throws IOException {
	    log.debug(String.format("Creating Get for %s", user));

	    Get g = new Get(Bytes.toBytes(user));
	    g.addFamily(INFO_FAM);
	    return g;
	  }

	private static class User extends
			com.myernore.e68.HBaseMessenger.model.User {
		private User(Result r) {
			this(r.getValue(INFO_FAM, USER_COL),
					r.getValue(INFO_FAM, NAME_COL));
		}

		private User(byte[] user, byte[] name) {
			this(Bytes.toString(user), Bytes.toString(name));
		}

		private User(String user, String name) {
			this.user = user;
			this.name = name;
		}
	}

}
