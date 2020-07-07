package my04_weibo;

import java.io.IOException;

public class Code_03_TestWeiBo {
    public static void main(String[] args) throws Exception {

        //初始化
        init();

        //1001发布微博
        Code_02_Dao.publishWeiBo("1001", "A1");

        //1002关注了1001，1003
        Code_02_Dao.addAttends("1002", "1001", "1003");

        //获取1002初始化页面
        Code_02_Dao.getInitPage("1002");
        System.out.println("*****************111*****************");

        //1003发布三条微博，同时1001发布两条微博
        Code_02_Dao.publishWeiBo("1003", "C1");
        Code_02_Dao.publishWeiBo("1003", "C2");
        Code_02_Dao.publishWeiBo("1003", "C3");
        Code_02_Dao.publishWeiBo("1001", "A2");
        Code_02_Dao.publishWeiBo("1001", "A3");

        //获取1002初始化页面
        Code_02_Dao.getInitPage("1002");
        System.out.println("*****************222*****************");

        //1002取消关注1003
        Code_02_Dao.deleteAttends("1002", "1003");

        //获取1002初始化页面
        Code_02_Dao.getInitPage("1002");
        System.out.println("*****************333*****************");

        //1002再次关注1003
        Code_02_Dao.addAttends("1002", "1003");

        //获取1002初始化页面
        Code_02_Dao.getInitPage("1002");
        System.out.println("*****************444*****************");

        //获取1001微博详情
        Code_02_Dao.getAllDetail("1001");

    }

    public static void init() throws IOException {

        //创建命名空间
        Code_01_Utils.createNameSpace(Code_00_Constants.NAMESPACE);

        //创建三张表
        Code_01_Utils.createTable(Code_00_Constants.CONTENT_TABLE, Code_00_Constants.CONTENT_TABLE_VERSIONS, Code_00_Constants.CONTENT_TABLE_CF);
        Code_01_Utils.createTable(Code_00_Constants.RELATION_TABLE, Code_00_Constants.RELATION_TABLE_VERSIONS, Code_00_Constants.RELATION_TABLE_CF1, Code_00_Constants.RELATION_TABLE_CF2);
        Code_01_Utils.createTable(Code_00_Constants.INBOX_TABLE, Code_00_Constants.INBOX_TABLE_VERSIONS, Code_00_Constants.INBOX_TABLE_CF);
    }

    /**
     * rowKey:1001_1594109174959column Family:infocolumn Qualifier:contentvalue:A1
     * *****************111*****************
     * rowKey:1001_1594109174959column Family:infocolumn Qualifier:contentvalue:A1
     * rowKey:1001_1594109175577column Family:infocolumn Qualifier:contentvalue:A3
     * rowKey:1003_1594109175435column Family:infocolumn Qualifier:contentvalue:C3
     * rowKey:1003_1594109175364column Family:infocolumn Qualifier:contentvalue:C2
     * *****************222*****************
     * rowKey:1001_1594109174959column Family:infocolumn Qualifier:contentvalue:A1
     * rowKey:1001_1594109175577column Family:infocolumn Qualifier:contentvalue:A3
     * *****************333*****************
     * rowKey:1001_1594109174959column Family:infocolumn Qualifier:contentvalue:A1
     * rowKey:1001_1594109175577column Family:infocolumn Qualifier:contentvalue:A3
     * rowKey:1003_1594109175435column Family:infocolumn Qualifier:contentvalue:C3
     * rowKey:1003_1594109175364column Family:infocolumn Qualifier:contentvalue:C2
     * *****************444*****************
     * rowKey:1001_1594109174959column Family:infocolumn Qualifier:contentvalue:A1
     * rowKey:1001_1594109175506column Family:infocolumn Qualifier:contentvalue:A2
     * rowKey:1001_1594109175577column Family:infocolumn Qualifier:contentvalue:A3
     */
}
