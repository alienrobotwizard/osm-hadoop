package com.infochimps.hadoop.osm;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
   
   A hadoop input format specifically for reading Open Street Map xml dumps (.osm dumps).
   <p>
   Yields one 'node', 'way', or 'relation' at a time. The hadoop key here will be the position
   in the underlying input stream and the values will be the raw xml of a record as text.
   
*/
public class OSMInputFormat extends TextInputFormat {

    public static final String OSM_NODE_START = "<node";
    public static final String OSM_WAY_START  = "<way";
    public static final String OSM_REL_START  = "<relation";
    public static final String SLASH          = "/";
    public static final String OPEN_TAG       = "<";
    public static final String CLOSE_TAG      = ">";
    
    public RecordReader<LongWritable,Text> createRecordReader(InputSplit inputSplit,
                                                              TaskAttemptContext context) {
        return new OSMRecordReader();
    }
  

    /**
       Reads one of an Open Street Map 'node', 'way', or 'relation'       
     */
    public static class OSMRecordReader extends RecordReader<LongWritable,Text> {

        private final static Log LOG = LogFactory.getLog(OSMRecordReader.class);
        
        private final byte[] nodeStart;
        private final byte[] wayStart;
        private final byte[] relStart;
        private final byte[] openTag;
        private final byte[] closeTag;
        private final byte[] slash;
        
        private long start;
        private long end;
        private FSDataInputStream fsin;
        private DataOutputBuffer buffer = new DataOutputBuffer();

        private int lastByte;

        private byte[] currentRecord;
        private LongWritable currentKey;
        private Text currentValue;
        
        public OSMRecordReader() {
            try {
                this.nodeStart = OSM_NODE_START.getBytes("utf-8");
                this.wayStart  = OSM_WAY_START.getBytes("utf-8");
                this.relStart  = OSM_REL_START.getBytes("utf-8");

                this.slash     = SLASH.getBytes("utf-8");
                this.openTag   = OPEN_TAG.getBytes("utf-8");
                this.closeTag  = CLOSE_TAG.getBytes("utf-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
        
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
            FileSplit fileSplit = (FileSplit)split;
            start = fileSplit.getStart();
            end   = start + fileSplit.getLength();
            Path file     = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(context.getConfiguration());
            fsin = fs.open(fileSplit.getPath());
            fsin.seek(start);
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (fsin.getPos() < end) {
                if (readUntilNext(false)) {
                    try {
                        buffer.write(currentRecord);
                        if (readUntilNext(true)) {
                            currentKey   = new LongWritable(fsin.getPos());
                            currentValue = new Text();
                            currentValue.set(buffer.getData(), 0, buffer.getLength());
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }

            }
            return false;
        }
    
        @Override
        public LongWritable getCurrentKey() {
            return currentKey;
        }
    
        @Override
        public Text getCurrentValue() {
            return currentValue;
        }
        
        @Override
        public void close() throws IOException {
            fsin.close();
        }
    
        @Override
        public float getProgress() throws IOException {
            return (fsin.getPos() - start) / (float) (end - start);
        }


        /**
           This function could gag a maggot. Either seeks the beginning of a record or
           reads a record depending on whether 'withinRecord' is true or false.

           @param withinRecord: Is the current position inside or outside a record?
           
         */
        private boolean readUntilNext(boolean withinRecord) throws IOException {
            
            int i = 0, j = 0, k = 0; // one counter per record type
            int openTags = 1;        // start with one open tag since we'll be inside a record
            
            while (true) {
                Integer b = fsin.read(); // read one byte from stream
                if (b==-1) return false; // eof

                if (withinRecord) {
                    if ((lastByte == openTag[0]) && (b != slash[0])) openTags++;
                    if (b == slash[0]) {
                        // read until '>'
                        buffer.write(b);
                        while(b != closeTag[0]) {
                            b = fsin.read();
                            if (b==-1) return false; // eof
                            buffer.write(b);
                            lastByte = b;
                        }
                        openTags--;
                    } else {
                        buffer.write(b);
                        lastByte = b;
                    }

                    if (openTags == 0) return true;
                }

                //
                // See if we find a record to read. Note that we have to check the three different kinds of possible
                // records here and thus the three different counters
                //
                if (b == nodeStart[i]) {
                    i++;
                    if (i >= nodeStart.length) {
                        currentRecord = nodeStart;
                        lastByte = b;
                        return true; // we've found a record to read
                    }
                } else i = 0;

                if (b == wayStart[j]) {
                    j++;
                    if (j >= wayStart.length) {
                        currentRecord = wayStart;
                        lastByte = b;
                        return true; // we've found a record to read
                    }
                } else j = 0;

                if (b == relStart[k]) {
                    k++;
                    if (k >= relStart.length) {
                        currentRecord = relStart;
                        lastByte = b;
                        return true; // we've found a record to read
                    }
                } else k = 0;
                
                // see if we've passed the stop point:
                if (!withinRecord && i == 0 && j == 0 && k == 0 && fsin.getPos() >= end) return false;
            }
        }
    }
}
