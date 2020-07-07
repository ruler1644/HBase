package my01_HbaseAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import static my01_HbaseAPI.Code_01_DDL.close;

/**
 * @Auther wu
 * @Date 2019/7/17  16:30
 */

//操作的表不一定是同一张表，所以每个方法内单独获取Table对象
public class Code_02_DML {

    public static Configuration conf;
    public static Connection connection;

    static {
        try {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "hadoop102");

            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        //put("test", "1001", "info", "name", "han");

        //get("test", "1001", "", "");
        //get("wu:stu", "1001", "info", "");
        //get("wu:stu", "1001", "info", "name");

        scan("test");
        //deleteData("studnet", "1001", "info", "age");
        close();
    }

    //5.向表中添加数据(多条/多列)
    public static void put(String tableName, String rowKey, String columnFamily,
                           String columnName, String value) throws Exception {

        //获取table对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        //给put对象赋值
        //添加多列(rowKey是相同的)方式一
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("sex"), Bytes.toBytes("woman"));

        //添加多列方式二
        //有多个put对象(rowKey是不相同的)
        //table.put(new ArrayList<Put>());

        table.put(put);

        //关闭资源
        table.close();
    }


    //6.查询数据，get方式
    public static void get(String tableName, String rowKey, String columnFamily,
                           String columnName) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        //获取一行
        Get get = new Get(Bytes.toBytes(rowKey));

        //获取指定列族的数据
        //get.addFamily(Bytes.toBytes(columnFamily));

        //获取指定列的数据
        //get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));

        //设置获取数据的版本数，表存储2个的话，设置获取5个最多也就返回2个
        get.setMaxVersions();
        get.setMaxVersions(5);

        Result result = table.get(get);

        //解析result
        for (Cell cell : result.rawCells()) {
            System.out.print("columnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell)) + "\t");
            System.out.print("columnName:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t");
            System.out.println("value:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        table.close();
    }

    //7.查询数据，scan方式
    public static void scan(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        //构建scan对象
        //Scan scan = new Scan(Bytes.toBytes("1001"), Bytes.toBytes("1002"));
        Scan scan = new Scan();

        ResultScanner scanner = table.getScanner(scan);

        //解析scanner(默认一次拉取500条数据)
        for (Result result : scanner) {

            //解析result
            for (Cell cell : result.rawCells()) {
                System.out.print("rowKey:" + Bytes.toString(CellUtil.cloneRow(cell)) + "\t");
                System.out.print("columnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell)) + "\t");
                System.out.print("columnName:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t");
                System.out.println("value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
    }

    //8.删除表中数据
    public static void deleteData(String tableName, String rowKey, String columnFamily,
                                  String columnName) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));


        //1111删除行
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        //2222删除列族
        //delete.addFamily(Bytes.toBytes(columnFamily));


        //3333删除列,使用addColumns(不指定TimeStamp)
        //Delete all versions of the specified column.
        delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        //Delete the latest version of the specified column
        //delete.addColumn();
        //3333删除列,使用addColumns(指定TimeStamp)
        //delete.addColumns(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),14398763044l);


        /*//4444删除列,使用addColumn(不指定TimeStamp)
        delete.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
        //4444删除列,使用addColumn(指定TimeStamp)
        delete.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),1439832645763044l);*/


        table.delete(delete);
    }
}