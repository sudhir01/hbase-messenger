package com.myernore.e68.hbasemessenger;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.HTablePool;

import com.myernore.e68.hbasemessenger.hbase.UsersDAO;

import utils.LoadUtils;


public class LoadUsers {

  public static final String usage =
    "loadusers \n" +
    "  help                :  print this message and exit.\n" +
    "  loadRomeoAndJuliet  :  load Romeo, Juliet, and Nurse users.\n";

  private static String randName(List<String> names) {
    String name = LoadUtils.randNth(names).trim() + " ";
    name += LoadUtils.randNth(names).trim();
    return name;
  }

  private static String randUser(String name) {
	int indexOfSpace = name.indexOf(" ");
	String finitial = name.substring(0,1);
	String lname = name.substring(indexOfSpace + 1, name.length());
    return String.format("%s%d", finitial + lname, LoadUtils.randInt(100)).toLowerCase();
  }

  public static void main(String[] args) throws IOException {
    if (args.length == 0 || "help".equals(args[0])) {
      System.out.println(usage);
      System.exit(0);
    }
    if( args[0].equals("loadRomeoAndJuliet")) {
       loadRomeoAndJulietUsers();
    }
  }

   private static void loadRomeoAndJulietUsers() throws IOException {
   HTablePool pool = new HTablePool();
   UsersDAO usersConnection = new UsersDAO(pool);
     usersConnection.addUser("Romeo", "Romeo Montague");
     usersConnection.addUser("Juliet", "Juliet Capulet");
     usersConnection.addUser("Nurse", "Nurse Capulet");
   pool.closeTablePool(UsersDAO.TABLE_NAME);
   System.out.println("Added 3 users.");
}
}
