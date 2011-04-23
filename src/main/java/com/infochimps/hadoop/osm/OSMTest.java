package com.infochimps.hadoop.osm;

import java.io.IOException;
import java.io.StringReader;
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

import org.xml.sax.InputSource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

public class OSMTest extends Configured implements Tool {

    private final static Log LOG = LogFactory.getLog(OSMTest.class);
    private final static DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();


    public static class OSMMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        DocumentBuilder docBuilder;
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            try {
                
                // Read the string value into an xml input source
                InputSource xmlSource = new InputSource();
                xmlSource.setCharacterStream(new StringReader(value.toString()));
                Document record = docBuilder.parse(xmlSource);

                //
                // TODO: Read the record and do interesting happy-fun-magic with it
                //
                
                context.write(NullWritable.get(), new Text(record.getDocumentElement().getTagName())); // foobar, test
                
            } catch (Exception e) {
                // Probably bad record, increment counter
            }
        }

        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
            try {
                this.docBuilder = dbf.newDocumentBuilder();
            } catch (Exception e) {
                e.printStackTrace();
            }

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
