package com.company.Utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;


public class TwoDPointWritable implements Writable {

    private FloatWritable x,y;

    public TwoDPointWritable() {
        this.x = new FloatWritable();
        this.y = new FloatWritable();
    }

    public void set ( float a, float b)
    {
        this.x.set(a);
        this.y.set(b);
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        x.readFields(in);
        y.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        x.write(out);
        y.write(out);
    }


    public FloatWritable getx() {
        return x;
    }

    public FloatWritable gety() {
        return y;
    }

}
