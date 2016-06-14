package com.company.Utils;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;


public class TwoDPointFileRecordReader  extends RecordReader<LongWritable, TwoDPointWritable>{

    LineRecordReader lineReader;
    TwoDPointWritable value;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext attempt)
            throws IOException, InterruptedException {
        lineReader = new LineRecordReader();
        lineReader.initialize(inputSplit, attempt);

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!lineReader.nextKeyValue())
        {
            return false;
        }
        Scanner reader  = new Scanner (new StringReader(lineReader.getCurrentValue().toString()));
        ArrayList<Float> tempData = new ArrayList<>();
        while(reader.hasNext()) {
            tempData.add(reader.nextFloat());
        }
        Float[] temp = new Float[tempData.size()];
        value = new TwoDPointWritable();
        value.set(tempData.toArray(temp));
        return true;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,
            InterruptedException {
        return lineReader.getCurrentKey();
    }

    @Override
    public TwoDPointWritable getCurrentValue() throws IOException,
            InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineReader.close();
    }

}