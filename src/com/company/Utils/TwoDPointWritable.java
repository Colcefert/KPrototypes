package com.company.Utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;


public class TwoDPointWritable implements Writable {

    private ArrayList<FloatWritable> fl;

    public TwoDPointWritable() {
        fl = new ArrayList<FloatWritable>();
    }

    public void set (Float ... args)
    {
        for(Float f1 : args){
            fl.add(new FloatWritable(f1));
        }
    }


    public void write(DataOutput out) throws IOException {
        for (FloatWritable flw : fl) {
            flw.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int i = 0;
        fl.clear();
        while(i < 2) {
            FloatWritable atom = new FloatWritable();
            atom.readFields(in);
            fl.add(atom);
            i++;
        }
    }


    public ArrayList<FloatWritable> getData() {
        return fl;
    }
}
