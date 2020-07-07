package my03_mapreduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Auther wu
 * @Date 2019/7/19  23:39
 */

public class Code_03_driver implements Tool {
    private Configuration configuration;

    public Configuration getConf() {
        return configuration;
    }

    public void setConf(Configuration conf) {
        configuration = conf;
    }

    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(configuration);
        job.setJarByClass(Code_03_driver.class);

        //设置mapper类及其输出的key和value类型
        TableMapReduceUtil.initTableMapperJob("fruit", new Scan(), Code_01_mapper.class, ImmutableBytesWritable.class, Put.class, job);

        TableMapReduceUtil.initTableReducerJob("fruit_2", Code_02_reducer.class, job);

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) {
        try {
            Configuration configuration = HBaseConfiguration.create();
            ToolRunner.run(configuration, new Code_03_driver(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
