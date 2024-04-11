import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseOperation {

    private static final String CF_DEFAULT = "cf";
    private static Connection connection;
    private static Admin admin;


    // 创建表，如果已存在则先删除再创建
    public static void createTable(String tableName, String[] fields) throws IOException {
        Configuration config = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();
        TableName tablename = TableName.valueOf(tableName);

        try {
            if (admin.tableExists(tablename)) {
                System.out.println("Table already exists!");
                return;
            }

            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tablename);

            for (String field : fields) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes(field))
                        .setMaxVersions(3)
                        .build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }

            TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
            admin.createTable(tableDescriptor);
            System.out.println("Table " + tableName + " created successfully.");

        } finally {
            close();
        }
    }
    private static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    // 向表中添加记录
    public static void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection();
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(row));
            for (int i = 0; i < fields.length; i++) {
                String[] parts = fields[i].split(":");
                String columnFamily = parts[0];
                String qualifier = parts[1];
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(values[i]));
            }
            table.put(put);
            System.out.println("Record added successfully to table " + tableName + ".");
        }
    }


    // 浏览指定列的数据
    public static void scanColumn(String tableName, String column) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection();
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            String[] parts = column.split(":");
            byte[] columnFamilyBytes = Bytes.toBytes(parts[0]);
            byte[] qualifierBytes = Bytes.toBytes(parts[1]);

            Scan scan = new Scan();
            scan.addColumn(columnFamilyBytes, qualifierBytes);

            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                byte[] valueBytes = result.getValue(columnFamilyBytes, qualifierBytes);
                if (valueBytes != null) {
                    String value = Bytes.toString(valueBytes);
                    System.out.println("Value: " + value);
                } else {
                    System.out.println("Value not found.");
                }
            }
        }
    }


    // 修改指定单元格的数据
    public static void modifyData(String tableName, String row, String column, String value) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection();
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(column.split(":")[0]), Bytes.toBytes(column.split(":")[1]), Bytes.toBytes(value));
            table.put(put);
            System.out.println("Data modified successfully.");
        }
    }

    // 删除指定行的记录
    public static void deleteRow(String tableName, String row) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection();
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            Delete delete = new Delete(Bytes.toBytes(row));
            table.delete(delete);
            System.out.println("Row deleted successfully from table " + tableName + ".");
        }
    }

    public static void main(String[] args) throws IOException {
        // 测试代码
        String tableName = "TestTable";
        String[] fields = {"Info", "Score:Math", "Score:Computer Science", "Score:English"};
        createTable("TestTable", new String[]{"Info", "ScoreMath", "ScoreComputerScience", "ScoreEnglish"});

        String row = "Zhangsan";
        String[] fieldNames = {"Info:S_No", "Info:S_Name", "Info:S_Sex", "Info:S_Age"};
        String[] fieldValues = {"2015001", "Zhangsan", "male", "23"};
        addRecord(tableName, row, fieldNames, fieldValues);

        scanColumn(tableName, "Info:S_Name");

        modifyData(tableName, row, "Info:S_Age", "24");

        deleteRow(tableName, row);
    }
}
