package com.myernore.e68.hbasemessenger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import com.myernore.e68.hbasemessenger.hbase.UsersDAO;


public class InitTables {

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);

    if (args.length > 0 && args[0].equalsIgnoreCase("-f")) {
      if (admin.tableExists(UsersDAO.TABLE_NAME)) {
        System.out.printf("Deleting %s\n", Bytes.toString(UsersDAO.TABLE_NAME));
        if (admin.isTableEnabled(UsersDAO.TABLE_NAME))
          admin.disableTable(UsersDAO.TABLE_NAME);
        admin.deleteTable(UsersDAO.TABLE_NAME);
      }
    }

    if (admin.tableExists(UsersDAO.TABLE_NAME)) {
      System.out.println("User table already exists.");
    } else {
      System.out.println("Creating User table...");
      HTableDescriptor desc = new HTableDescriptor(UsersDAO.TABLE_NAME);
      desc.addFamily(new HColumnDescriptor(UsersDAO.INFO_FAM));
      desc.addFamily(new HColumnDescriptor(UsersDAO.MSGS_FAM));
      admin.createTable(desc);
      
      System.out.println("User table created.");
    }
    
    admin.close();
  }
}
