package com.myernore.e68.hbasemessenger;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.hadoop.hbase.client.HTablePool;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.myernore.e68.hbasemessenger.hbase.UsersDAO;
import com.myernore.e68.hbasemessenger.model.User;
import com.myernore.e68.hbasemessenger.model.User.Message;

public class MessagesTool {

	private static String USAGE = "Usage - MessagesTool\n"
			+ "  MessagesTool help                                       :  print this help message\n"
			+ "  MessagesTool list                                       :  list usernames to message\n"
			+ "  MessagesTool check <username1>                          :  check messages for \n"
			+ "  MessagesTool msg <username1> <username2> \"<message>\"  :  sends \"message\" from username1 to username2";

	public static void main(String[] args) throws IOException {
		if (args.length < 1 || args[0].equals("help")) {
			printUsage();
		}
		if (args[0].equals("list")) {
			UsersTool.printUsers();
		} else if (args[0].equals("check") && args.length == 2) {
			checkMessages(args[1]);
		} else if (args[0].equals("msg") && args.length == 4) {
			sendMessage(args[1], args[2], args[3]);
		} else {
			printUsage();
		}

	}

	private static void checkMessages(String username) throws IOException {
		HTablePool pool = new HTablePool();
		UsersDAO usersDao = new UsersDAO(pool);
		User user1 = usersDao.getUser(username);
		checkForNoUsername(username, user1);
		DateTimeFormatter dtf = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss");
		if (user1 == null) {
			System.out.println("Exiting.");
			System.exit(-1);
		}
		System.out.println("Checking messages for " + user1.name + ".");
		for (Message m : user1.getMessages()) {
			boolean isFrom = m.fromUser.username.equals(username);
			String fromOrTo = isFrom ? "To " + m.toUser.username
					: "From " +  m.fromUser.username;
			System.out.println("-----------------------\n" + fromOrTo + ":");
			System.out.println("(" + dtf.print(m.date) + ")");
			System.out.println(m.body);
		}
		pool.closeTablePool(UsersDAO.TABLE_NAME);
		if( user1.getMessages().size() < 1 ) {
			System.out.println("There were no messages.");
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
		checkForNoUsername(fromUsername, user1);
		checkForNoUsername(fromUsername, user2);
		if (user1 == null || user2 == null) {
			System.out.println("Exiting.");
			System.exit(-1);
		}
		usersDao.addMessage(user1, user2, message);
		pool.closeTablePool(UsersDAO.TABLE_NAME);
		System.out.println("Message sent.");
	}

	private static void checkForNoUsername(String fromUsername, User user1) {
		String notFound = " not found in system.";
		if (user1 == null) {
			System.out.println(fromUsername + notFound);
		}
	}
}
