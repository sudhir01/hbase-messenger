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

import utils.Md5Utils;

import com.myernore.e68.hbasemessenger.model.User;
import com.myernore.e68.hbasemessenger.model.User.Message;

public class UsersDAO {

	private static final Logger log = Logger.getLogger(UsersDAO.class);
	private static final int longLength = 8; // length of long, in bytes, for
												// timestamps

	public static final byte[] TABLE_NAME = Bytes.toBytes("hbm-users");
	public static final byte[] INFO_FAM = Bytes.toBytes("info");
	public static final byte[] MSGS_FAM = Bytes.toBytes("msgs");

	// ///////////// Info Column Family
	public static final byte[] USER_COL = Bytes.toBytes("username");
	public static final byte[] NAME_COL = Bytes.toBytes("name");

	// //////////// Messages Column Family
	public static final byte[] FROM_MSG_COL_SUFFIX = Bytes.toBytes("fr");
	public static final byte[] TO_MSG_COL_SUFFIX = Bytes.toBytes("to");
	public static final byte[] MSG_INFO_SEPARATOR = Bytes.toBytes("-");

	/** 
	 * This works fine, but it breaks Hue browser, so I decided to change to a string-based approach. 
	 * @param msg
	 * @param isFrom
	 * @return
	 */
	public static byte[] makeToMessageColumnNameBytes(UsersDAOUser.MessagesDAO msg,
			boolean isFrom) {
		
		// reverse timestamp-username-from
		byte[] timestamp = Bytes.toBytes(-1 * msg.date.getMillis());
		byte[] username = Bytes.toBytes(isFrom ? msg.fromUser.username
				: msg.toUser.username);
		byte[] suffix = isFrom ? FROM_MSG_COL_SUFFIX : TO_MSG_COL_SUFFIX;
		int suffixLen = isFrom ? FROM_MSG_COL_SUFFIX.length
				: TO_MSG_COL_SUFFIX.length;

		byte[] colKey = new byte[longLength + MSG_INFO_SEPARATOR.length
				+ username.length + MSG_INFO_SEPARATOR.length + suffixLen];

		int offset = 0;
		// https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/util/Bytes.html#putBytes(byte[],
		// int, byte[], int, int)
		offset = Bytes.putBytes(colKey, offset, timestamp, 0, timestamp.length);
		offset = Bytes.putBytes(colKey, offset, MSG_INFO_SEPARATOR, 0,
				MSG_INFO_SEPARATOR.length);
		offset = Bytes.putBytes(colKey, offset, username, 0, username.length);
		offset = Bytes.putBytes(colKey, offset, MSG_INFO_SEPARATOR, 0,
				MSG_INFO_SEPARATOR.length);
		Bytes.putBytes(colKey, offset, suffix, 0, suffixLen);
		return colKey;
	}
	
	/**
	 * This is slower, but more understandable from the hbase shell
	 * @param msg
	 * @param isForFromUser
	 * @return
	 */
	public static byte[] makeToMessageColumnNameString(UsersDAOUser.MessagesDAO msg,
			boolean isForFromUser) {
		
		// [reverse timestamp]-[username]-[fr]om or [reverse timestamp]-[username]-[to]
		String ts = String.format("%0" + longLength + "d",-1 * msg.date.getMillis() );
		String un = isForFromUser ? msg.toUser.username : msg.fromUser.username;
		String suffix = isForFromUser ? "to" : "fr";
				
		return Bytes.toBytes(ts + '-' + un + '-' + suffix);
	}

	private HTablePool pool;

	public UsersDAO(HTablePool pool) {
		this.pool = pool;
	}

	public boolean addUser(String username, String name) throws IOException {
		HTableInterface userTable = pool.getTable(TABLE_NAME);

		Put p = mkPut(new UsersDAOUser(username, name));

		// make sure that username doesn't already exist by using checkAndPut
		// and null for the username.
		boolean updatedName = userTable.checkAndPut(Bytes.toBytes(username),
				INFO_FAM, USER_COL, null, p);

		userTable.close();
		String msg = String.format("Creating Put for %s", username);
		if (updatedName) {
			log.debug(msg + " succeeded.");
		} else {
			log.debug(msg
					+ " failed. This username probably already exists in the database, so didn't add it again.");
		}
		return updatedName;

	}

	private static Put mkPut(UsersDAOUser u) {
		log.debug(String.format("Creating Put for %s", u));

		Put p = new Put(Bytes.toBytes(u.username));
		p.add(INFO_FAM, USER_COL, Bytes.toBytes(u.username));
		if (u.name != null && u.name.length() > 0) {
			p.add(INFO_FAM, NAME_COL, Bytes.toBytes(u.name));
		}

		return p;
	}

	// private static void addToPut(Put p, Message m) {
	// p.add
	// }

	private static Get mkGet(String user) throws IOException {
		log.debug(String.format("Creating Get for %s", user));
		Get g = new Get(Bytes.toBytes(user));
		g.addFamily(INFO_FAM);
		return g;
	}

	private static class UsersDAOUser extends User {

		private UsersDAOUser(User u) {
			super(u.username);
			this.name = u.name == null ? "" : u.name;
		}
		
		private UsersDAOUser(Result r) {
			this(r.getValue(INFO_FAM, USER_COL), r.getValue(INFO_FAM, NAME_COL));
		}

		private UsersDAOUser(byte[] username) {
			super(Bytes.toString(username));
		}

		private UsersDAOUser(byte[] user, byte[] name) {
			this(Bytes.toString(user), Bytes.toString(name));
		}

		private UsersDAOUser(String username, String name) {
			super(username);
			this.name = name;
		}

		public UsersDAOUser(String username) {
			super(username);
		}

		private class MessagesDAO extends User.Message {

			public MessagesDAO(String body, String fromUsername,
					String toUsername) {
				super(body, fromUsername, toUsername);
			}

			public MessagesDAO(UsersDAOUser fromUser, UsersDAOUser toUser,
					String message) {
				super(fromUser, toUser, message);
			}

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
	 * users. Currently does not support error checking to see if users exist; 
	 * that is, will try to message users that don't exist. 
	 * 
	 * @param fromUsername
	 * @param toUsername
	 * @param string
	 * @throws IOException
	 */
	public void addMessage(String fromUsername, String toUsername,
			String message) throws IOException {
		UsersDAOUser fromUser = new UsersDAOUser(fromUsername);
		UsersDAOUser toUser = new UsersDAOUser(toUsername);
		addMessage(fromUser, toUser, message);
	}

	private Put mkPut(
			com.myernore.e68.hbasemessenger.hbase.UsersDAO.UsersDAOUser.MessagesDAO msg,
			boolean isForFromUser) {

		String username = isForFromUser ? msg.fromUser.username : msg.toUser.username;
		log.debug(String
				.format("Creating Put for new message between %s and %s, storing in user %s",
						msg.fromUser.username, msg.toUser.username, username));
		Put p = new Put(Bytes.toBytes(username));
		p.add(MSGS_FAM, makeToMessageColumnNameString(msg, isForFromUser),
				Bytes.toBytes(msg.body));

		return p;
	}

	public User getUser(String username) throws IOException {
		HTableInterface users = pool.getTable(TABLE_NAME);
		Get get = mkGet(username);
		Result result = users.get(get);
	    if (result.isEmpty()) {
	      log.info(String.format("user %s not found.", username));
	      return null;
	    }

	    User u = new UsersDAOUser(result);
	    users.close();
	    return u;
	}

	public void addMessage(User fromUser, User toUser, String message) throws IOException {
		addMessage(new UsersDAOUser( fromUser), new UsersDAOUser( toUser ), message);
	}
	
	private void addMessage(UsersDAOUser fromUser, UsersDAOUser toUser, String message) throws IOException {
		UsersDAOUser.MessagesDAO msg = fromUser.new MessagesDAO(fromUser,
				toUser, message);
		Put putIntoFromUser = mkPut(msg, true);
		Put putIntoToUser = mkPut(msg, false);

		HTableInterface userTable = pool.getTable(TABLE_NAME);
		userTable.put(putIntoFromUser);
		userTable.put(putIntoToUser);
		userTable.close();
	}

	

}
