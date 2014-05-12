package com.myernore.e68.hbasemessenger;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.HTablePool;


import com.myernore.e68.hbasemessenger.hbase.UsersDAO;
import com.myernore.e68.hbasemessenger.model.User;

public class UsersTool {
	private static String USAGE = "Usage - UsersTool\n"
			+ "  UsersTool help                                :  print this help message\n"
			+ "  UsersTool list                                :  list users\n"
			+ "  UsersTool add username1 \"User DisplayName\"  :  creates user with username1 and \"User DisplayName\"";

	public static void main(String[] args) throws IOException {
		if ( args.length < 1 || args[0].equals("help")) {
			printUsage();
		}
		if( args[0].equals("list") ) {
			printUsers();
		} else if( args[0].equals("add") && args.length == 3 ) {
			addUser(args[1], args[2]);
		} else {
			printUsage();
		}
	}

	 static void addUser(String username, String name) throws IOException {
		HTablePool pool = new HTablePool();
		UsersDAO usersConnection = new UsersDAO(pool);

		boolean success = usersConnection.addUser(username, name);
		pool.closeTablePool(UsersDAO.TABLE_NAME);

		if(success) {
			System.out.println(String.format("Added new user %s", username));
		} else {
			System.out.println("Unable to add user - perhaps a user with that username already exists?");
		}
	}
	
	private static void printUsage() {
		System.out.println("UsersTool has required parameters.");
		System.out.println(USAGE);
		System.exit(0);
	}
	
    static void printUsers() throws IOException {
		HTablePool pool = new HTablePool();
		UsersDAO usersDao = new UsersDAO(pool);
		
		List<User> users = usersDao.getUsers();
		for( User u : users ) {
			System.out.println(u);
		}
		pool.closeTablePool(UsersDAO.TABLE_NAME);
	}
}
