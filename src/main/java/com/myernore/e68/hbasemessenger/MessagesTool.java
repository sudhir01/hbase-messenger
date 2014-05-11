package com.myernore.e68.hbasemessenger;

import java.io.IOException;
import org.apache.hadoop.hbase.client.HTablePool;

import com.myernore.e68.hbasemessenger.hbase.UsersDAO;

public class MessagesTool {

	private static String USAGE = "Usage - MessagesTool\n"
			+ "  MessagesTool -h  :  print this help message\n"
			+ "  MessagesTool <username1> <username2> \"<message>\"  :  sends \"message\" from username1 to username2";

	public static void main(String[] args) throws IOException {
		if (args.length != 3 || args[0].equals("-h")) {
			System.out.println(USAGE);
			System.exit(0);
		}
		sendMessage(args[0], args[1], args[2]);
	}

	public static void sendMessage(String fromUsername, String toUsername,
			String message) throws IOException {
		HTablePool pool = new HTablePool();
		UsersDAO usersDao = new UsersDAO(pool);
		usersDao.addMessage(fromUsername, toUsername, message);
		pool.closeTablePool(UsersDAO.TABLE_NAME);
	}
}
