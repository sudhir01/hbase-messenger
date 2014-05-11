package com.myernore.e68.hbasemessenger;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.HTablePool;

import com.myernore.e68.hbasemessenger.hbase.UsersDAO;
import com.myernore.e68.hbasemessenger.model.User;

import utils.LoadUtils;

public class LoadMessages {

	public static final String usage = "loadmessages username count\n"
			+ "  help - print this message and exit.\n"
			+ "  username - insert random messages from specified HBase user"
			+ "  count - add count random HBaseMessenger messages.\n";

	private static String randMessage(List<String> words) {
		String message = "";
		for (int i = 0; i < 20; i++) {
			message += LoadUtils.randNth(words) + " ";
		}
		return message;
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 2 || "help".equals(args[0])) {
			System.out.println(usage);
			System.exit(0);
		}
		String fromUser = args[0];
		int numMessages = 0;
		try {
			Integer.parseInt(args[1]);
			generateMessagesFrom(fromUser,numMessages);
		} catch ( NumberFormatException nfe ) {
			System.out.println(args[1] + " is not a valid number of messages. Exiting.");
		} 
	}

	private static void generateMessagesFrom(String fromUser, int numMessages) throws IOException {
		HTablePool pool = new HTablePool();
		UsersDAO usersDao = new UsersDAO(pool);
		List<String> words = LoadUtils.readResource(LoadUtils.WORDS_PATH);
		Iterator<User> users = usersDao.getUsers().iterator();
		int numMessagesSent = 0;
		while (numMessagesSent <  numMessages) {
			if( ! users.hasNext() ) {
				users = usersDao.getUsers().iterator();
			}
			User toUser= users.next();
				usersDao.addMessage(fromUser, toUser.name, words.toString());
		}
		pool.closeTablePool(UsersDAO.TABLE_NAME);
	}
}
