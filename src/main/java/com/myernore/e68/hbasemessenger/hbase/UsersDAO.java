package com.myernore.e68.hbasemessenger.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.myernore.e68.hbasemessenger.model.User;
import com.myernore.e68.hbasemessenger.model.User.Message;

public class UsersDAO {

	private static final Logger log = Logger.getLogger(UsersDAO.class);

	public static final byte[] TABLE_NAME = Bytes.toBytes("hbm-users");
	public static final byte[] INFO_FAM = Bytes.toBytes("info");
	public static final byte[] MSGS_FAM = Bytes.toBytes("msgs");

	// Info Column Family Columns
	public static final byte[] USER_COL = Bytes.toBytes("username");
	public static final byte[] NAME_COL = Bytes.toBytes("name");

	// Messages Column Family Columns

	private HTablePool pool;

	public UsersDAO(HTablePool pool) {
		this.pool = pool;
	}

	public void addUser(String username, String name) throws IOException {
		HTableInterface userTable = pool.getTable(TABLE_NAME);
		
		
		Put p = mkPut(new UsersDAOUser(username, name));
		
		// make sure that username doesn't already exist by using checkAndPut and null for the username.
		boolean updatedName = userTable.checkAndPut(Bytes.toBytes(username), INFO_FAM, USER_COL, null, p);

		userTable.close();
		String msg = String.format("Creating Put for %s", username);
		if (updatedName ) {
			log.debug( msg + " succeeded.");
		} else {
			log.debug( msg + " failed. This username probably already exists in the database, so didn't add it again.");			
		}

	}

	private static Put mkPut(UsersDAOUser u) {
		log.debug(String.format("Creating Put for %s", u));

		Put p = new Put(Bytes.toBytes(u.username));
		p.add(INFO_FAM, USER_COL, Bytes.toBytes(u.username));
		if (u.name != null && u.name.length() > 0) {
			p.add(INFO_FAM, NAME_COL, Bytes.toBytes(u.name));
		}

//		if (u.messages != null && u.messages.size() > 0) {
//			for(Message m : u.messages) {
//				addToPut(p, m);
//			}
//		}
		return p;
	}

//	private static void addToPut(Put p, Message m) {
//		p.add
//	}

	private static Get mkGet(String user) throws IOException {
		log.debug(String.format("Creating Get for %s", user));
		Get g = new Get(Bytes.toBytes(user));
		g.addFamily(INFO_FAM);
		return g;
	}

	private static class UsersDAOUser extends User {
		private UsersDAOUser(Result r) {
			this(r.getValue(INFO_FAM, USER_COL), r.getValue(INFO_FAM, NAME_COL));
		}

		private UsersDAOUser(byte[] username) {
			this(Bytes.toString(username));
		}
		
		private UsersDAOUser(String username) {
			this.username = username;
		}

		private UsersDAOUser(byte[] user, byte[] name) {
			this(Bytes.toString(user), Bytes.toString(name));
		}

		private UsersDAOUser(String username, String name) {
			this.username = username;
			this.name = name;
		}
	}

	public List<User> getUsers() throws IOException {
		HTableInterface users = pool.getTable(TABLE_NAME);
		ResultScanner results = users.getScanner(mkScan());
		ArrayList<User> ret = new ArrayList<User>();
		for (Result r : results) {
			ret.add(new UsersDAOUser(r));
		}

		users.close();
		return ret;
	}

	private static Scan mkScan() {
		Scan s = new Scan();
		s.addFamily(INFO_FAM);
		return s;
	}

	/**
	 * Adds a message fromUsername to toUsername. Message is added to both
	 * users.
	 * 
	 * @param fromUsername
	 * @param toUsername
	 * @param string
	 */
//	public void addMessage(String fromUsername, String toUsername, String string) {
//		HTableInterface userTable = pool.getTable(TABLE_NAME);
//
//		Put p = mkPut(new UsersDAOUser(fromUsername, name));
//		userTable.put(p);
//
//		userTable.close();
//	}

}
