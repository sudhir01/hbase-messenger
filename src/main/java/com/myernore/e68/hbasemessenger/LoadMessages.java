package com.myernore.e68.hbasemessenger;

import java.io.IOException;
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

		HTablePool pool = new HTablePool();
		UsersDAO usersConnection = new UsersDAO(pool);

		String fromUser = args[0];
		int numMessages = Integer.parseInt(args[1]);

		List<String> words = LoadUtils.readResource(LoadUtils.WORDS_PATH);
		List<User> users = usersConnection.getUsers();

		
//		for (int i = 0; i < numMessages;) {
//			for (User u : users) {
//				usersConnection.addMessage(fromUser, u.name, words.toString());
//			}
//			usersConnection.addUser(fromUser, user.name);
//		}

		pool.closeTablePool(UsersDAO.TABLE_NAME);
		throw new UnsupportedOperationException("Not Implemented for Load Messages yet");
	}
}
