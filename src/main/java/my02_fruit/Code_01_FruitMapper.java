package my02_fruit;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/7/19  15:17
 * @describe 通过hadoop将本地文件写入HBase表
 */
public class Code_01_FruitMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        context.write(key, value);
    }
}
