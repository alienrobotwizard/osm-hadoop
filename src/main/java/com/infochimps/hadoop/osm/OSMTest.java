package com.infochimps.hadoop.osm;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class OSMTest extends Configured implements Tool {

    private final static Log LOG = LogFactory.getLog(OSMTest.class);
    
    public static class OSMMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String newVal = "record ["+value.toString()+"]";
            context.write(NullWritable.get(), new Text(newVal));
        }
    }

    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(OSMTest.class);
        job.setJobName("OSMTest");
        job.setMapperClass(OSMMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setInputFormatClass(OSMInputFormat.class);

        List<String> other_args = new ArrayList<String>();
        for (int i=0; i < args.length; ++i) {
            System.out.println(args[i]);
            other_args.add(args[i]);
        }
        // Here we need _both_ an input path and an output path.
        // Output stores failed records so they can be re-indexed
        FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
    
        try {
            job.waitForCompletion(true);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OSMTest(), args);
        System.exit(res);
    }
}
