package my04_weibo;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @Auther wu
 * @Date 2019/7/25  0:14
 * 1<---->2删除微博
 */
public class Code_02_Dao {

    //1.发布微博(微博内容表添加数据，B的粉丝的收件箱表添加数据)
    public static void publishWeiBo(String uid, String context) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Code_00_Constants.CONFIGURATION);

        //TODO 步骤一：操作微博内容表Content
        Table contTable = connection.getTable(TableName.valueOf(Code_00_Constants.CONTENT_TABLE));
        long timeStamp = System.currentTimeMillis();
        String rowKey = uid + "_" + timeStamp;
        Put put = new Put(Bytes.toBytes(rowKey));

        //创建put对象，put数据
        put.addColumn(Bytes.toBytes(Code_00_Constants.INBOX_TABLE_CF), Bytes.toBytes("content"), Bytes.toBytes(context));
        contTable.put(put);

        //TODO 步骤二：查看relation表，遍历微博发布者的粉丝，更新粉丝们的收件箱表信息
        Table relationTable = connection.getTable(TableName.valueOf(Code_00_Constants.RELATION_TABLE));

        //获取当前发布微博用户的fans列族信息
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(Code_00_Constants.RELATION_TABLE_CF2));
        Result result = relationTable.get(get);

        //创建list集合，存放put对象，每一个put对象，都是收件箱表inbox中B的粉丝的一条记录
        ArrayList<Put> inboxPuts = new ArrayList<>();

        //遍历fans，给收件箱表的put对象赋值
        for (Cell cell : result.rawCells()) {
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));
            inboxPut.addColumn(Bytes.toBytes(Code_00_Constants.INBOX_TABLE_CF), Bytes.toBytes(uid), Bytes.toBytes(rowKey));
            inboxPuts.add(inboxPut);
        }

        //存在粉丝时，将他新发送的微博，推送粉丝的收件箱
        if (inboxPuts.size() > 0) {
            Table indexTable = connection.getTable(TableName.valueOf(Code_00_Constants.INBOX_TABLE));
            indexTable.put(inboxPuts);
            indexTable.close();
        }

        //关闭资源
        relationTable.close();
        contTable.close();
        connection.close();
    }

    //2.关注用户(A的attends添加BCD，同时为BCD添加粉丝A，最后在收件箱表inbox中，A有BCD各自最新的三条记录)
    public static void addAttends(String uid, String... attends) throws IOException {

        if (attends.length < 0) {
            System.out.println("请选择要关注的人！！！");
            return;
        }
        Connection connection = ConnectionFactory.createConnection(Code_00_Constants.CONFIGURATION);

        //TODO 步骤一：操作relation表
        Table relationTable = connection.getTable(TableName.valueOf(Code_00_Constants.RELATION_TABLE));

        //2.创建一个集合，用于存放relation表的Put对象，一个操作者的put对象，多个被关注者的put对象(A关注BCD有4个put对象)
        ArrayList<Put> relationPuts = new ArrayList<Put>();
        Put uidPut = new Put(Bytes.toBytes(uid));

        for (String attend : attends) {

            //给操作者的Put对象赋值
            uidPut.addColumn(Bytes.toBytes(Code_00_Constants.RELATION_TABLE_CF1), Bytes.toBytes(attend), Bytes.toBytes(attend));

            //被关注者的Put对象赋值
            Put attendPut = new Put(Bytes.toBytes(attend));
            attendPut.addColumn(Bytes.toBytes(Code_00_Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid), Bytes.toBytes(uid));

            //将被关注者的Put对象放入集合
            relationPuts.add(attendPut);
        }

        //将操作者的Put对象添加至集合
        relationPuts.add(uidPut);
        relationTable.put(relationPuts);

        //TODO 步骤二：操作收件箱表，addAttends(String uid, String... attends)
        //获取微博内容表Content对象
        Table contentTable = connection.getTable(TableName.valueOf(Code_00_Constants.CONTENT_TABLE));

        //创建收件箱表Inbox的put对象
        Put inboxPut = new Put(Bytes.toBytes(uid));

        //循环遍历attends，获取每个被关注者近期发布的微博
        for (String attend : attends) {

            //获取当前被关注者，近期发布的微博( scan扫描startRow=>B_ ,stopRow=>B| )
            Scan scan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend + "|"));
            ResultScanner scanner = contentTable.getScanner(scan);

            //定义时间戳
            long timeStamp = System.currentTimeMillis();

            //循环遍历ResultScanner，给收件箱表的put对象赋值
            for (Result result : scanner) {

                //获取rowKey
                byte[] rowKey = result.getRow();
                inboxPut.addColumn(Bytes.toBytes(Code_00_Constants.INBOX_TABLE_CF), Bytes.toBytes(attend), timeStamp++, rowKey);

            }
        }

        //判断put对象是否为空
        if (!inboxPut.isEmpty()) {

            //获取收件箱表对象，写入数据
            Table inboxTable = connection.getTable(TableName.valueOf(Code_00_Constants.INBOX_TABLE));
            inboxTable.put(inboxPut);
            inboxTable.close();
        }

        //关闭资源
        relationTable.close();
        contentTable.close();
        connection.close();
    }

    //3.取关用户
    public static void deleteAttends(String uid, String... delAttends) throws Exception {

        Connection connection = ConnectionFactory.createConnection(Code_00_Constants.CONFIGURATION);

        //TODO 步骤一：操作用户关系表
        Table relationTable = connection.getTable(TableName.valueOf(Code_00_Constants.RELATION_TABLE));
        ArrayList<Delete> deleteList = new ArrayList<>();
        Delete uidDelete = new Delete(Bytes.toBytes(uid));

        //循环创建被取消关注的delete对象
        for (String delAttend : delAttends) {
            uidDelete.addColumns(Bytes.toBytes(Code_00_Constants.RELATION_TABLE_CF1), Bytes.toBytes(delAttend));
            Delete attendDelete = new Delete(Bytes.toBytes(delAttend));

            //为被取消关注者的delete对象赋值
            attendDelete.addColumns(Bytes.toBytes(Code_00_Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid));
            deleteList.add(attendDelete);
        }
        deleteList.add(uidDelete);
        relationTable.delete(deleteList);

        //TODO 步骤二：操作收件箱表
        Table inboxTable = connection.getTable(TableName.valueOf(Code_00_Constants.INBOX_TABLE));
        Delete inboxDelete = new Delete(Bytes.toBytes(uid));

        //A要删除BCD发布的微博
        for (String delAttend : delAttends) {
            inboxDelete.addColumns(Bytes.toBytes(Code_00_Constants.INBOX_TABLE_CF), Bytes.toBytes(delAttend));
        }
        inboxTable.delete(inboxDelete);

        //关闭资源
        relationTable.close();
        inboxTable.close();
        connection.close();
    }

    //4.获取用户的初始化页面
    public static void getInitPage(String uid) throws IOException {

        Connection connection = ConnectionFactory.createConnection(Code_00_Constants.CONFIGURATION);

        //TODO 操作收件箱表
        Table inboxTable = connection.getTable(TableName.valueOf(Code_00_Constants.INBOX_TABLE));
        Table contentTable = connection.getTable(TableName.valueOf(Code_00_Constants.CONTENT_TABLE));

        //创建收件箱表get对象，并设置最大版本
        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.setMaxVersions();
        Result result = inboxTable.get(inboxGet);
        for (Cell cell : result.rawCells()) {
            byte[] rowKey = CellUtil.cloneValue(cell);

            //TODO 操作微博内容表
            Get contentGet = new Get(rowKey);
            Result contentResult = contentTable.get(contentGet);
            for (Cell contentCell : contentResult.rawCells()) {
                System.out.println("rowKey:" + Bytes.toString(CellUtil.cloneRow(contentCell))
                        + ", column Family:" + Bytes.toString(CellUtil.cloneFamily(contentCell))
                        + ", column Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(contentCell))
                        + ", value:" + Bytes.toString(CellUtil.cloneValue(contentCell)));
            }
        }

        //关闭资源
        inboxTable.close();
        contentTable.close();
        connection.close();
    }

    // 5.点击查看某个用户所有的微博
    public static void getAllDetail(String uid) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Code_00_Constants.CONFIGURATION);
        Table contentTable = connection.getTable(TableName.valueOf(Code_00_Constants.CONTENT_TABLE));
        Scan scan = new Scan();

        //TODO 方式一：scan扫描( startRow=>B_,stopRow=>B| )
        //TODO 方式二：构建过滤器
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "_"));
        scan.setFilter(rowFilter);

        //解析结果数据
        ResultScanner scanner = contentTable.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("rowKey:" + Bytes.toString(CellUtil.cloneRow(cell))
                        + ", column Family:" + Bytes.toString(CellUtil.cloneFamily(cell))
                        + ", column Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + ", value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        //关闭资源
        contentTable.close();
        connection.close();
    }
}