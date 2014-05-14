package com.myernore.e68.hbasemessenger;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import com.myernore.e68.hbasemessenger.hbase.UsersDAO;

public class InitTables {

   private static HBaseAdmin getHBaseAdmin() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
      Configuration conf = HBaseConfiguration.create();
      return new HBaseAdmin(conf);
   }
   
   public static void dropAndCreateTables() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
      HBaseAdmin admin = getHBaseAdmin();
      dropTables(admin);
      createTables(admin);
      admin.close();
   }

   public static void main(String[] args) throws Exception {
      if (args.length > 0 && args[0].equalsIgnoreCase("-f")) {
         dropAndCreateTables();
      } else {
         createTables();
      }
   }

   private static void createTables() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
      HBaseAdmin admin = getHBaseAdmin();
      createTables(admin);
      admin.close();
   }

   /**
    * @param admin
    * @throws IOException
    */
   private static void dropTables(HBaseAdmin admin) throws IOException {

      if (admin.tableExists(UsersDAO.TABLE_NAME)) {
         System.out
               .printf("Deleting %s\n", Bytes.toString(UsersDAO.TABLE_NAME));
         if (admin.isTableEnabled(UsersDAO.TABLE_NAME))
            admin.disableTable(UsersDAO.TABLE_NAME);
         admin.deleteTable(UsersDAO.TABLE_NAME);
      }

   }

   /**
    * @param admin
    * @throws IOException
    */
   private static void createTables(HBaseAdmin admin) throws IOException {
      if (admin.tableExists(UsersDAO.TABLE_NAME)) {
         System.out.println("User table already exists.");
      } else {
         System.out.println("Creating User table...");
         HTableDescriptor desc = new HTableDescriptor(UsersDAO.TABLE_NAME);
         desc.addFamily(new HColumnDescriptor(UsersDAO.COL_FAM_INFO));
         desc.addFamily(new HColumnDescriptor(UsersDAO.COL_FAM_MSGS));
         admin.createTable(desc);

         System.out.println("User table created.");
      }
   }
}
