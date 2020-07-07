package my04_weibo;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/7/24  23:43
 */

public class Code_01_Utils {

    //创建命名空间
    public static void createNameSpace(String nameSpace) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Code_00_Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        admin.createNamespace(namespaceDescriptor);
        admin.close();
        connection.close();
    }

    //判断表是否存在
    public static boolean tableExists(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Code_00_Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();
        boolean flag = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        connection.close();
        return flag;
    }

    //创建表(三张表)
    public static void createTable(String tableName, int versions, String... columnFamilys) throws IOException {

        if (tableExists(tableName)) {
            System.out.println(tableName + "表已存在！！！");
            return;
        }
        if (columnFamilys.length <= 0) {
            System.out.println("请设置列族信息！！！");
            return;
        }

        Connection connection = ConnectionFactory.createConnection(Code_00_Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();

        //创建表描述器，循环添加列族信息
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String columnFamily : columnFamilys) {

            //创建列族描述器，设置保存的数据版本数目
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
            hColumnDescriptor.setMaxVersions(versions);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
        admin.close();
        connection.close();
    }
}