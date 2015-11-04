package com.epam.preprod.hadoop.prez2;

import com.epam.preprod.hadoop.extension.PairOutputWritable;
import com.epam.preprod.hadoop.extension.PairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
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

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        conf.setBoolean("mapreduce.output.fileoutputformat.compress", true );
        conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");



        Job job = Job.getInstance(conf, "Log analyzer");

        job.setJarByClass(AccessLogDriver.class);

        job.setMapperClass(AccessLogMapper.class);
        job.setReducerClass(AccessLogReducer.class);
        job.setCombinerClass(AccessLogCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairOutputWritable.class);



        TextInputFormat.addInputPath(job, new Path(args[0]));
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
