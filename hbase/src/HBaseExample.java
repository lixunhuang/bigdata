import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseExample {

    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();
        try {
            // 连接 HBase
            Connection connection = ConnectionFactory.createConnection(config);

            // 添加记录
            addRecord(connection, "Student", "scofield", 45, 89, 100);

            // 获取 scofield 的 English 成绩信息
            int englishScore = getEnglishScore(connection, "Student", "scofield");
            System.out.println("Scofield's English Score: " + englishScore);

            // 关闭连接
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 添加记录
    private static void addRecord(Connection connection, String tableName, String studentName,
                                  int englishScore, int mathScore, int computerScore) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(studentName));
        put.addColumn(Bytes.toBytes("English"), Bytes.toBytes(""), Bytes.toBytes(Integer.toString(englishScore)));
        put.addColumn(Bytes.toBytes("Math"), Bytes.toBytes(""), Bytes.toBytes(Integer.toString(mathScore)));
        put.addColumn(Bytes.toBytes("Computer"), Bytes.toBytes(""), Bytes.toBytes(Integer.toString(computerScore)));
        table.put(put);
        table.close();
    }

    // 获取指定学生的 English 成绩信息
    private static int getEnglishScore(Connection connection, String tableName, String studentName)
            throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(studentName));
        get.addColumn(Bytes.toBytes("English"), Bytes.toBytes(""));
        Result result = table.get(get);
        byte[] valueBytes = result.getValue(Bytes.toBytes("English"), Bytes.toBytes(""));
        if (valueBytes != null) {
            return Integer.parseInt(Bytes.toString(valueBytes));
        } else {
            return -1; // 如果成绩为空，则返回 -1
        }
    }
}
