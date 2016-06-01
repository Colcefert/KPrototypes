package com.company.Utils;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class TwoDPointFileInputFormat extends FileInputFormat<LongWritable, TwoDPointWritable>{

    @Override
    public RecordReader<LongWritable, TwoDPointWritable> createRecordReader(
            InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException {
        return new TwoDPointFileRecordReader();
    }

}
