package com.hajimei.mapreduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class HajimeiMapReduceMain {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration Configuration=new Configuration();
        Job job=Job.getInstance(Configuration);

        job.setJarByClass(HajimeiMapReduceMain.class);
        job.setMapperClass(HajimeiMapper.class);
        job.setReducerClass(HajimeiReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(HajimeiFlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(HajimeiFlowBean.class);
        //job.setNumberReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result=job.waitForCompletion(true);
        System.exit(result?0:1);

    }
}
