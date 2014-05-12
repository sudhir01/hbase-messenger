package com.myernore.e68.hbasemessenger;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.HTablePool;

import com.myernore.e68.hbasemessenger.hbase.UsersDAO;
import com.myernore.e68.hbasemessenger.model.User;

public class MessagesTool {

	private static String USAGE = "Usage - MessagesTool\n"
			+ "  MessagesTool help  :  print this help message\n"
			+ "  MessagesTool list  :  list usernames to message\n"
			+ "  MessagesTool msg   :  <username1> <username2> \"<message>\"  :  sends \"message\" from username1 to username2";

	public static void main(String[] args) throws IOException {
		if ( args.length < 1 || args[0].equals("help")) {
			printUsage();
		}
		if( args[0].equals("list") ) {
			UsersTool.printUsers();
		} else if( args[0].equals("msg") && args.length == 4 ) {
			sendMessage(args[1], args[2], args[3]);
		} else {
			printUsage();
		}
		
	}

	private static void printUsage() {
		System.out.println("MessagesTool has required parameters.");
		System.out.println(USAGE);
		System.exit(0);
	}

	

	public static void sendMessage(String fromUsername, String toUsername,
			String message) throws IOException {
		HTablePool pool = new HTablePool();
		UsersDAO usersDao = new UsersDAO(pool);
		User user1 = usersDao.getUser(fromUsername);
		User user2 = usersDao.getUser(toUsername);
		String notFound = " not found in system.";
		if( user1 == null ) {
			System.out.println(fromUsername + notFound);
		}
		if( user2 == null ) {
			System.out.println(toUsername + notFound);
		}
		if( user1 == null || user2 == null ) {
			System.out.println("Exiting.");
			System.exit(-1);
		}
		usersDao.addMessage(user1, user2, message);
		pool.closeTablePool(UsersDAO.TABLE_NAME);
		System.out.println("Message sent.");
	}
}
