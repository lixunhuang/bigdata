import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;

import java.io.IOException;
import java.util.List;

public class HBaseListTables {
    private static Connection connection;
    private static Admin admin;

    public static void main(String[] args) {
        try {
            listTables();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    public static void listTables() throws IOException {
        init();
        List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
        for (TableDescriptor tableDescriptor : tableDescriptors) {
            TableName tableName = tableDescriptor.getTableName();
            System.out.println("Table: " + tableName);
        }
    }

    public static void init() throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "zookeeper_server");
        connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();
    }

    public static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
