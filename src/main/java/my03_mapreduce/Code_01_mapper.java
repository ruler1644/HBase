package my03_mapreduce;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/7/19  23:38
 */
public class Code_01_mapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
            throws IOException, InterruptedException {

        Put put = new Put(key.get());

        //获取数据
        for (Cell cell : value.rawCells()) {

            //判断是否是name列
            String columnQualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            if ("name".equals(columnQualifier)) {
                put.add(cell);
            }
        }
        context.write(key, put);
    }
}