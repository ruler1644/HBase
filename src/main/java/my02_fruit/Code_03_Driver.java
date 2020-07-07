package my02_fruit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Auther wu
 * @Date 2019/7/19  15:18
 */

//需要两个参数，第一个是输入数据位置，第二个是数据写出的表
public class Code_03_Driver implements Tool {

    private Configuration configuration;


    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(configuration);
        job.setJarByClass(Code_03_Driver.class);

        //设置mapper类及其输出的key和value类型
        job.setMapperClass(Code_01_FruitMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        //设置Reducer类
        TableMapReduceUtil.initTableReducerJob(args[1], Code_02_FruitReducer.class, job);

        //设置输入参数，提交任务
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public Configuration getConf() {
        return configuration;
    }

    public void setConf(Configuration conf) {
        configuration = conf;
    }

    public static void main(String[] args) {
        try {
            Configuration configuration = new Configuration();
            ToolRunner.run(configuration, new Code_03_Driver(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
