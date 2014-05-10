package com.myernore.e68.HbaseMessenger;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.HTablePool;

import com.myernore.e68.HbaseMessenger.hbase.UserConnection;

import utils.LoadUtils;


public class LoadUsers {

  public static final String usage =
    "loadusers count\n" +
    "  help - print this message and exit.\n" +
    "  count - add count random TwitBase users.\n";

  private static String randName(List<String> names) {
    String name = LoadUtils.randNth(names) + " ";
    name += LoadUtils.randNth(names);
    return name;
  }

  private static String randUser(String name) {
    return String.format("%s%2d", name.substring(5), LoadUtils.randInt(100));
  }

  private static String randEmail(String user, List<String> words) {
    return String.format("%s@%s.com", user, LoadUtils.randNth(words));
  }

  public static void main(String[] args) throws IOException {
    if (args.length == 0 || "help".equals(args[0])) {
      System.out.println(usage);
      System.exit(0);
    }

    HTablePool pool = new HTablePool();
    UserConnection usersConnection = new UserConnection(pool);

    int count = Integer.parseInt(args[0]);
    List<String> names = LoadUtils.readResource(LoadUtils.NAMES_PATH);
    List<String> words = LoadUtils.readResource(LoadUtils.WORDS_PATH);

    for (int i = 0; i < count; i++) {
      String name = randName(names);
      String username = randUser(name);
      usersConnection.addUser(username, name);
    }

    pool.closeTablePool(UserConnection.TABLE_NAME);
  }
}
