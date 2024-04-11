import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.io.IOException;

public class CreateHBaseTable {

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("your_table_name");

            // 列族描述符
            ColumnFamilyDescriptor personalCF = ColumnFamilyDescriptorBuilder.of("personal_data");
            ColumnFamilyDescriptor professionalCF = ColumnFamilyDescriptorBuilder.of("professional_data");

            // 表描述符
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamily(personalCF)
                    .setColumnFamily(professionalCF)
                    .build();

            // 创建表
            admin.createTable(tableDescriptor);
            System.out.println("Table created successfully.");
        }
    }
}
