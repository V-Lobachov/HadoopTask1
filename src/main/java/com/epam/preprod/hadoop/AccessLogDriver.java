package com.epam.preprod.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Volodymyr_Lobachov on 11/3/2015.
 */
public class AccessLogDriver extends Configured implements Tool {
    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new Configuration(), new AccessLogDriver(), args);
        System.exit(res);
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Job job = new Job(conf, "Configuration writer");



        return job.waitForCompletion(true) ? 0 : 1;
    }
}
