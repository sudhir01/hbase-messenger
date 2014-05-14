package com.myernore.e68.hbasemessenger.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.myernore.e68.hbasemessenger.model.User;

public class UsersDAO {

   private static class UsersDAOUser extends User {

      private class MessagesDAO extends User.Message {

         public MessagesDAO(byte[] dt, String username2, boolean isFrom,
               byte[] body) {
            super(new User(username2), isFrom,
            // multiply by -1 since it was in reverse chron
                  new DateTime(-1 * Bytes.toLong(dt)), Bytes.toString(body));
         }

         public MessagesDAO(UsersDAOUser user2, boolean isFrom, String message) {
            super(user2, isFrom, message);
         }

      }

      private Result r;

      public UsersDAOUser(String username) {
         super(username);
      }

      private UsersDAOUser(byte[] username) {
         super(Bytes.toString(username));
      }

      private UsersDAOUser(Result r) {
         this(r.getValue(COL_FAM_INFO, COL_FAM_INFO_COL_USER));
         this.r = r;
         this.name = Bytes.toString(r.getValue(COL_FAM_INFO,
               COL_FAM_INFO_COL_NAME));
         final byte[] numMessagesInBytes = r.getValue(COL_FAM_INFO,
               COL_FAM_INFO_COL_NUM_MSGS) == null ? Bytes.toBytes(0L) : r
               .getValue(COL_FAM_INFO, COL_FAM_INFO_COL_NUM_MSGS);
         this.numMessages = Bytes.toLong(numMessagesInBytes);
         unmarshalMessages(r);
      }

      private UsersDAOUser(String username, String name) {
         super(username);
         this.name = name;
      }

      private UsersDAOUser(User u) {
         super(u.username);
         this.name = u.name == null ? "" : u.name;
      }

      private void unmarshalMessages(Result r2) {
         final NavigableMap<byte[], byte[]> familyMap = this.r
               .getFamilyMap(COL_FAM_MSGS);

         for (final byte[] columnKey : familyMap.keySet()) {

            final byte[] dt = Arrays.copyOfRange(columnKey, 0, LONG_LENGTH);
            final int usernameStartIndex = LONG_LENGTH
                  + COL_FAM_MSGS_SEPARATOR.length;
            final byte[] usernameAndFrom = Arrays.copyOfRange(columnKey,
                  usernameStartIndex, columnKey.length);
            final String usernameAndFromString = Bytes
                  .toString(usernameAndFrom);
            final int fromStringIndex = usernameAndFromString.length()
                  - (COL_FAM_MSGS_SUFFIX_FROM.length + COL_FAM_MSGS_SEPARATOR.length)
                  + 1;
            final String username = usernameAndFromString.substring(0,
                  fromStringIndex - 1);
            final boolean isFrom = usernameAndFromString.endsWith(Bytes
                  .toString(COL_FAM_MSGS_SUFFIX_FROM));
            final byte[] body = r2.getValue(COL_FAM_MSGS, columnKey);
            final MessagesDAO msg = new MessagesDAO(dt, username, isFrom, body);
            addMessage(msg);
         }
      }
   }

   public static final byte[] COL_FAM_INFO = Bytes.toBytes("info");
   public static final byte[] COL_FAM_INFO_COL_NAME = Bytes.toBytes("name");
   public static final byte[] COL_FAM_INFO_COL_USER = Bytes.toBytes("username");
   public static final byte[] COL_FAM_INFO_COL_NUM_MSGS = Bytes
         .toBytes("numMessages");

   public static final byte[] COL_FAM_MSGS = Bytes.toBytes("msgs");
   public static final byte[] COL_FAM_MSGS_SEPARATOR = Bytes.toBytes("-");
   public static final byte[] COL_FAM_MSGS_SUFFIX_FROM = Bytes.toBytes("fr");
   public static final byte[] COL_FAM_MSGS_SUFFIX_TO = Bytes.toBytes("to");

   public static final byte[] TABLE_NAME = Bytes.toBytes("hbm-users");
   private static final Logger LOG = LoggerFactory.getLogger(UsersDAO.class);
   private static final int LONG_LENGTH = 8; // long length for timestamps

   /**
    * This worked fine, but the raw hex characters break Hue browser in cloudera
    * 4 VM, so I decided to change to a string-based approach that would be
    * easier to present.
    * 
    * @param msg
    * @param isFrom
    * @return
    */
   public static byte[] makeToMessageColumnNameBytes(
         UsersDAOUser.MessagesDAO msg) {

      // reverse timestamp-username-from
      final byte[] timestamp = Bytes.toBytes(-1 * msg.date.getMillis());
      final byte[] username = Bytes.toBytes(msg.getOtherUser().username);
      final byte[] suffix = msg.isFrom ? COL_FAM_MSGS_SUFFIX_TO
            : COL_FAM_MSGS_SUFFIX_FROM;

      final byte[] colKey = new byte[LONG_LENGTH
            + COL_FAM_MSGS_SEPARATOR.length + username.length
            + COL_FAM_MSGS_SEPARATOR.length + suffix.length];

      int offset = 0;
      // https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/util/Bytes.html#putBytes(byte[],
      // int, byte[], int, int)
      offset = Bytes.putBytes(colKey, offset, timestamp, 0, timestamp.length);
      offset = Bytes.putBytes(colKey, offset, COL_FAM_MSGS_SEPARATOR, 0,
            COL_FAM_MSGS_SEPARATOR.length);
      offset = Bytes.putBytes(colKey, offset, username, 0, username.length);
      offset = Bytes.putBytes(colKey, offset, COL_FAM_MSGS_SEPARATOR, 0,
            COL_FAM_MSGS_SEPARATOR.length);
      Bytes.putBytes(colKey, offset, suffix, 0, suffix.length);
      return colKey;
   }

   /**
    * Using string to parse the column qualifiers is slower than using byte
    * arrays, but it is more understandable for a tutorial like this because
    * they can be interpreted from the HBase Shell and from the Hue HBase
    * Browser.
    * 
    * @param msg
    * @param isForFromUser
    * @return
    */
   public static byte[] makeToMessageColumnNameString(
         UsersDAOUser.MessagesDAO msg) {

      // [reverse timestamp]-[username]-[fr]om or [reverse
      // timestamp]-[username]-[to]
      final String ts = String.format("%0" + LONG_LENGTH + "d",
            -1 * msg.date.getMillis());
      final String un = msg.getOtherUser().username;
      final String suffix = msg.isFrom ? "to" : "fr";

      return Bytes.toBytes(ts + '-' + un + '-' + suffix);
   }

   private static Get mkGet(String user) throws IOException {
      LOG.debug(String.format("Creating Get for %s", user));
      final Get g = new Get(Bytes.toBytes(user));
      g.addFamily(COL_FAM_INFO);
      g.addFamily(COL_FAM_MSGS);
      return g;
   }

   private static Increment mkIncrement(
         com.myernore.e68.hbasemessenger.hbase.UsersDAO.UsersDAOUser.MessagesDAO msg) {
      final String username = msg.getHostUser().username;
      final Increment i = new Increment(Bytes.toBytes(username));
      i.addColumn(COL_FAM_INFO, COL_FAM_INFO_COL_NUM_MSGS, 1L);
      return i;
   }

   /**
    * Creates the PUT API call for HBase for modifying the users associated with
    * a message so that both users store the sent message. Calls
    * makeToMessageColumnNameBytes(msg) to generate the column name to store the
    * body in, and stores that column in the COL_FAM_MSGS column family.
    * Increments the number of msgs for that user.
    * 
    * @param msg
    * @return
    */
   private static Put mkPut(
         com.myernore.e68.hbasemessenger.hbase.UsersDAO.UsersDAOUser.MessagesDAO msg) {

      final String username = msg.getHostUser().username;
      final Put p = new Put(Bytes.toBytes(username));
      // byte[] column family, byte[] column qualifier, byte[] value
      p.add(COL_FAM_MSGS, makeToMessageColumnNameBytes(msg),
            Bytes.toBytes(msg.body));

      return p;
   }

   /**
    * Creates the PUT api call to Hbase for creating or modifying a User.
    * 
    * @param u
    * @return
    */
   private static Put mkPut(UsersDAOUser u) {
      LOG.debug(String.format("Creating Put for %s", u));

      final Put p = new Put(Bytes.toBytes(u.username));
      p.add(COL_FAM_INFO, COL_FAM_INFO_COL_USER, Bytes.toBytes(u.username));
      if (u.name != null && u.name.length() > 0) {
         p.add(COL_FAM_INFO, COL_FAM_INFO_COL_NAME, Bytes.toBytes(u.name));
      }

      return p;
   }

   private static Scan mkScan() {
      final Scan s = new Scan();
      s.addFamily(COL_FAM_INFO);
      return s;
   }

   private final HTablePool pool;

   public UsersDAO(HTablePool pool) {
      this.pool = pool;
   }

   /**
    * Adds a message fromUsername to toUsername. Message is added to both users.
    * Currently does not support error checking to see if users exist; that is,
    * will try to message users that don't exist.
    * 
    * @param fromUsername
    * @param toUsername
    * @param string
    * @throws IOException
    */
   public void addMessage(String fromUsername, String toUsername, String message)
         throws IOException {
      final UsersDAOUser fromUser = new UsersDAOUser(fromUsername);
      final UsersDAOUser toUser = new UsersDAOUser(toUsername);
      addMessage(fromUser, toUser, message);
   }

   public void addMessage(User fromUser, User toUser, String message)
         throws IOException {
      addMessage(new UsersDAOUser(fromUser), new UsersDAOUser(toUser), message);
   }

   public boolean addUser(String username, String name) throws IOException {
      final HTableInterface userTable = this.pool.getTable(TABLE_NAME);

      final Put p = mkPut(new UsersDAOUser(username, name));

      // make sure that username doesn't already exist by using checkAndPut
      // and null for the username.
      final boolean updatedName = userTable.checkAndPut(
            Bytes.toBytes(username), COL_FAM_INFO, COL_FAM_INFO_COL_USER, null,
            p);

      userTable.close();
      final String msg = String.format("Creating Put for %s", username);
      if (updatedName) {
         LOG.debug(msg + " succeeded.");
      } else {
         LOG.debug(msg
               + " failed. This username probably already exists in the database, so didn't add it again.");
      }
      return updatedName;

   }

   public User getUser(String username) throws IOException {
      final HTableInterface users = this.pool.getTable(TABLE_NAME);
      final Get get = mkGet(username);
      final Result result = users.get(get);
      if (result.isEmpty()) {
         LOG.info(String.format("user %s not found.", username));
         return null;
      }

      final User u = new UsersDAOUser(result);
      users.close();
      return u;
   }

   public List<User> getUsers() throws IOException {
      final HTableInterface users = this.pool.getTable(TABLE_NAME);
      final ResultScanner results = users.getScanner(mkScan());
      final ArrayList<User> ret = new ArrayList<User>();
      for (final Result r : results) {
         ret.add(new UsersDAOUser(r));
      }

      users.close();
      return ret;
   }

   private void addMessage(UsersDAOUser fromUser, UsersDAOUser toUser,
         String message) throws IOException {
      final UsersDAOUser.MessagesDAO msgToStoreInFromUser = fromUser.new MessagesDAO(
            toUser, true, message);

      final UsersDAOUser.MessagesDAO msgToStoreInToUser = toUser.new MessagesDAO(
            fromUser, false, message);

      final Put putIntoFromUser = mkPut(msgToStoreInFromUser);
      final Increment incrementNumMessagesForFromUser = mkIncrement(msgToStoreInFromUser);

      final HTableInterface userTable = this.pool.getTable(TABLE_NAME);
      userTable.put(putIntoFromUser);
      userTable.increment(incrementNumMessagesForFromUser);

      // If Romeo has an aside and messages himself, don't add it twice or
      // count it twice
      if (!fromUser.username.equals(toUser.username)) {
         final Put putIntoToUser = mkPut(msgToStoreInToUser);
         userTable.put(putIntoToUser);
         final Increment incrementNumMessagesForToUser = mkIncrement(msgToStoreInToUser);
         userTable.increment(incrementNumMessagesForToUser);
      }

      userTable.close();
   }

}
