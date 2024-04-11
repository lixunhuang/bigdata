import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;


import java.io.IOException;

public class HBaseOperations {
    private static Configuration config = HBaseConfiguration.create();
    private static Connection connection;
    private static Admin admin;

    // ① 列出 HBase 所有表的相关信息，如表名、创建时间等。
    public static void init() throws IOException {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();
    }
    public static void listTables() throws IOException {
        init();
        TableName[] tableNames = admin.listTableNames();
        for (TableName tableName : tableNames) {
            TableDescriptor tableDescriptor = admin.getDescriptor(tableName);
            System.out.println("Table: " + tableName);

            // Get table creation time
//            long creationTime = tableDescriptor.getDurability().ordinal();
//            System.out.println("Creation Time: " + creationTime);

            ColumnFamilyDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
            for (ColumnFamilyDescriptor columnFamily : columnFamilies) {
                System.out.println("Column Family: " + columnFamily.getNameAsString());
            }
            // You can add more details about the table here
        }
    }

    // ② 在终端输出指定表的所有记录数据。
    public static void scanTable(String tableName) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf(tableName))) {

            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            boolean isEmpty = true;
            for (Result result : scanner) {
                isEmpty = false;
                System.out.println(Bytes.toString(result.getRow()) + ": " + result);
                // 获取列族为 cf，列名为 col1 对应的值
                byte[] valueBytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("col1"));
                if (valueBytes != null) {
                    String value = Bytes.toString(valueBytes);
                    System.out.println("值为：" + value);
                } else {
                    System.out.println("值不存在");
                }
            }
            if (isEmpty) {
                System.out.println("表 " + tableName + " 是空的。");
            }
        }
    }




    // ③ 向已经创建好的表添加和删除指定的列族或列。
    public static void addOrDeleteColumn(String tableName, String columnFamily, String column, String choose) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            TableName tn = TableName.valueOf(tableName);
            if (admin.tableExists(tn)) {
                // 获取修改前的列信息
                System.out.println("修改前的列信息：");
                printTableColumns(tableName);

                if (choose.equals("add")) {
                    // 用户选择添加列族
                    admin.addColumnFamily(tn, ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily)).build());
                    System.out.println("Column family '" + columnFamily + "' added successfully to table '" + tableName + "'.");
                } else {
                    // 用户选择删除列族
                    admin.deleteColumnFamily(tn, Bytes.toBytes(columnFamily));
                    System.out.println("Column family '" + columnFamily + "' deleted successfully from table '" + tableName + "'.");
                }

                // 获取修改后的列信息
                System.out.println("修改后的列信息：");
                printTableColumns(tableName);
            } else {
                System.out.println("Table '" + tableName + "' does not exist.");
            }
        }
    }

    public static void printTableColumns(String tableName) throws IOException {
        TableName tn = TableName.valueOf(tableName);
        TableDescriptor tableDescriptor = admin.getDescriptor(tn);
        System.out.println("Table: " + tableName);
        ColumnFamilyDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
        for (ColumnFamilyDescriptor columnFamily : columnFamilies) {
            System.out.println("Column Family: " + columnFamily.getNameAsString());
        }
        // You can add more details about the table here
    }


    // ④ 清空指定表的所有记录数据。
    public static void truncateTable(String tableName) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            TableName tn = TableName.valueOf(tableName);
            System.out.println("清空前的列信息：");
            scanTable(tableName);
            if (admin.tableExists(tn)) {
                if (!admin.isTableDisabled(tn)) { // 检查表是否已经禁用
                    admin.disableTable(tn);
                    // 等待一段时间，确保禁用表的操作完成
//                    Thread.sleep(2000); // 2秒
                    // 再次检查表的状态
                    if (!admin.isTableDisabled(tn)) {
                        System.err.println("Table " + tableName + " could not be disabled.");
                        return;
                    }
                }
                admin.truncateTable(tn, true);
                System.out.println("清空后的列信息：");
                scanTable(tableName);
                // admin.enableTable(tn);
            } else {
                System.out.println("Table " + tableName + " does not exist.");
            }
        }
    }


    // ⑤ 统计表的行数。
    public static long countRows(String tableName) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            Scan scan = new Scan();
            try (ResultScanner scanner = table.getScanner(scan)) {
                long rowCount = 0;
                for (Result result : scanner) {
                    rowCount++;
                }
                return rowCount;
            }
        }
    }

    public static void main(String[] args) throws IOException {
        // 测试各个功能
        listTables();
        scanTable("t2");
        //addOrDeleteColumn("t1", "add_column_family", "add_column", "dl");
        truncateTable("t1");
        System.out.println("Row count: " + countRows("t2"));
    }
}
