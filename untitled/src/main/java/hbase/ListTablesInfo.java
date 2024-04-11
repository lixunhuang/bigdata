package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.io.IOException;

public class ListTablesInfo {

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Admin admin = connection.getAdmin();
            TableDescriptor[] tables = admin.listTableDescriptors().toArray(new TableDescriptor[0]);
            for (TableDescriptor table : tables) {
                System.out.println("Table Name: " + table.getTableName());
                System.out.println("Created Time: " + table.getPriority());
                // Add more information as needed
            }
            admin.close();
        }
    }
}
