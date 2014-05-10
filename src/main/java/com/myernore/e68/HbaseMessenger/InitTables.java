package com.myernore.e68.HbaseMessenger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import com.myernore.e68.HbaseMessenger.hbase.UserConnection;


public class InitTables {

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);

    if (args.length > 0 && args[0].equalsIgnoreCase("-f")) {
      if (admin.tableExists(UserConnection.TABLE_NAME)) {
        System.out.printf("Deleting %s\n", Bytes.toString(UserConnection.TABLE_NAME));
        if (admin.isTableEnabled(UserConnection.TABLE_NAME))
          admin.disableTable(UserConnection.TABLE_NAME);
        admin.deleteTable(UserConnection.TABLE_NAME);
      }
    }

    if (admin.tableExists(UserConnection.TABLE_NAME)) {
      System.out.println("User table already exists.");
    } else {
      System.out.println("Creating User table...");
      HTableDescriptor desc = new HTableDescriptor(UserConnection.TABLE_NAME);
      desc.addFamily(new HColumnDescriptor(UserConnection.INFO_FAM));
      desc.addFamily(new HColumnDescriptor(UserConnection.MSGS_FAM));
      desc.addFamily(new HColumnDescriptor(UserConnection.MSGS_META_FAM));
      admin.createTable(desc);
      
      System.out.println("User table created.");
    }
  }
}
