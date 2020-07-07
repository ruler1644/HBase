package my01_HbaseAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/7/17  12:37
 */
public class Code_01_DDL {

    public static Connection connection;
    public static Admin admin;

    //静态代码块，初始化配置信息
    static {
        try {

            //获取配置信息
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

            //获取连接对象
            connection = ConnectionFactory.createConnection(conf);

            //获取admin对象
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {

        System.out.println(tableExist("stu"));

        //createTable("stu","info");

        //deleteTable("stu");

        createNameSpace("wu-hao");

        close();

    }

    //1.判断表是否存在
    public static boolean tableExist(String tableName) throws Exception {
        boolean flag = admin.tableExists(TableName.valueOf(tableName));
        return flag;
    }

    //2.创建表
    public static void createTable(String tableName, String... cfs) throws Exception {
        if (tableExist(tableName)) {
            System.out.println("表已经存在");
            return;
        }
        if (cfs.length < 1) {
            System.out.println("请输入列族信息");
            return;
        }

        //创建表描述器HTableDescriptor
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        for (String columnFamily : cfs) {

            //创建列族描述器HColumnDescriptor
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        admin.createTable(hTableDescriptor);
    }

    //3.删除表
    public static void deleteTable(String tableName) throws Exception {
        if (!tableExist(tableName)) {
            System.out.println(tableName + "表不存在");
            return;
        }

        //使表下线
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }

    //4.创建命名空间
    public static void createNameSpace(String nameSpace) {

        //命名空间描述器NameSpaceDescriptor////
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();

        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            System.out.println(nameSpace + "命名空间已经存在");
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("over");
    }

    //关闭资源
    public static void close() {
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
}
