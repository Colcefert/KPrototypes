package com.company.Utils.IO.Readable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by rvr on 12.07.16.
 */
public class DataObjectsFileInputFormat extends FileInputFormat<NullWritable, DataObjects> {

        @Override
        public RecordReader<NullWritable, DataObjects> createRecordReader(
                InputSplit arg0, TaskAttemptContext arg1) throws IOException,
                InterruptedException {
            return new DataObjectsRecordReader();
        }

}