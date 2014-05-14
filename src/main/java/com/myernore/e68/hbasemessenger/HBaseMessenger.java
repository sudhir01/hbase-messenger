package com.myernore.e68.hbasemessenger;

import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class HBaseMessenger {
   private static String USAGE = "Usage - HBaseMessenger\n"
         + "Testing Application:\n"
         + "  HBaseMessenger init\n"
         + "  HBaseMessenger list\n"
         + "  HBaseMessenger check Romeo\n"
         + "  HBaseMessenger check Juliet\n"
         + "  HBaseMessenger check Nurse\n"
         + "  HBaseMessenger add myusername1 \"My Displayname1\"\n"
         + "  HBaseMessenger msg Romeo myusername1 \"Welcome to HBase Messenger\"\n"
         + "  HBaseMessenger check myusername1\n"
         + "All Commands Available:\n"
         + "  HBaseMessenger add <username1> \"User DisplayName\"       :  creates username1 with name \"User DisplayName\"\n"
         + "  HBaseMessenger check <Romeo|Juliet|Nurse>                 :  check messages for Romeo, Juliet, or Nurse\n"
         + "  HBaseMessenger check <username1>                          :  check messages for \n"
         + "  HBaseMessenger help                                       :  print this help message\n"
         + "  HBaseMessenger init                                       :  wipes and initializes test database\n"
         + "  HBaseMessenger list                                       :  lists users in system\n"
         + "  HBaseMessenger msg <username1> <username2> \"<message>\"  :  sends \"message\" from username1 to username2\n";

   public static void main(String[] args) throws IOException {
      if (args.length < 1 || args[0].equals("help")) {
         printUsage();
      }
      // get rid of zookeeper info messages
      // http://stackoverflow.com/a/16749847/78202
      Logger.getRootLogger().setLevel(Level.ERROR);
      if (args[0].equals("add") && args.length == 3) {
         UsersAPI.addUser(args[1], args[2]);
      } else if (args[0].equals("check") && args.length == 2) {
         MessagesAPI.checkMessages(args[1]);
      } else if (args[0].equals("init")) {
         InitTables.dropAndCreateTables();
         LoadUsers.loadRomeoAndJulietUsers();
         LoadMessages.loadRomeoAndJulietMessages();
      } else if (args[0].equals("list")) {
         UsersAPI.printUsers();
      } else if (args[0].equals("msg") && args.length == 4) {
         MessagesAPI.sendMessage(args[1], args[2], args[3]);
      } else {
         printUsage();
      }
   }

   private static void printUsage() {
      System.out.println("UsersAPI has required parameters.");
      System.out.println(USAGE);
      System.exit(0);
   }

}
