package com.company.Utils.IO.Writable.MapOutput;

import org.apache.hadoop.io.*;
import tl.lin.data.map.HashMapWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by rvr on 13.07.16.
 */
public class ClustersMetrics implements Writable{
    TwoDArrayWritable attrSum;
    ArrayWritable numOfMembers;
    HashMapWritable<Text, IntWritable>[][] freqOfAttr;

    public ClustersMetrics(){
        attrSum = new TwoDArrayWritable(FloatWritable.class);
        numOfMembers = new ArrayWritable(IntWritable.class);
    }

    public ClustersMetrics(Float[][] clustAttrSum, HashMap<Text, IntWritable>[][] freqOfCatAttr, int[] numOfClusterMembers){
        int k = clustAttrSum.length;
        int n = clustAttrSum[0].length;
        FloatWritable[][] clAtrSum = new FloatWritable[k][n];
        for (int l = 0; l < k; l++) {
            for (int j = 0; j < n; j++) {
                clAtrSum[l][j] = new FloatWritable(clustAttrSum[l][j]);
            }
        }
        attrSum = new TwoDArrayWritable(FloatWritable.class, clAtrSum);


        int m = freqOfCatAttr[0].length;
        freqOfAttr = (HashMapWritable<Text, IntWritable>[][]) new HashMapWritable<?,?>[k][m];
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < m; j++) {
                freqOfAttr[i][j] = new HashMapWritable<Text, IntWritable>(freqOfCatAttr[i][j]);
            }
        }

        IntWritable[] numOfObjects = new IntWritable[numOfClusterMembers.length];
        for (int j = 0; j < numOfClusterMembers.length; j++) {
            numOfObjects[j] = new IntWritable(numOfClusterMembers[j]);
        }
        numOfMembers = new ArrayWritable(IntWritable.class, numOfObjects);
    }

    public ClustersMetrics(TwoDArrayWritable attrSum, ArrayWritable numOfMembers){
        this.attrSum = attrSum;
        this.numOfMembers = numOfMembers;
    }

    public void write(DataOutput out) throws IOException{
        attrSum.write(out);



        out.writeInt(freqOfAttr.length);

        int i;
        for(i = 0; i < freqOfAttr.length; ++i) {
            out.writeInt(this.freqOfAttr[i].length);
        }

        for(i = 0; i < this.freqOfAttr.length; ++i) {
            for(int j = 0; j < this.freqOfAttr[i].length; ++j) {
                this.freqOfAttr[i][j].write(out);
            }
        }

        numOfMembers.write(out);
    }

    public void readFields(DataInput in) throws IOException{
        attrSum.readFields(in);

        this.freqOfAttr = (HashMapWritable<Text, IntWritable>[][]) new HashMapWritable<?,?>[in.readInt()][];

        int i;
        for(i = 0; i < this.freqOfAttr.length; ++i) {
            this.freqOfAttr[i] = (HashMapWritable<Text, IntWritable>[]) new HashMapWritable<?,?>[in.readInt()];
        }

        for(i = 0; i < this.freqOfAttr.length; ++i) {
            for(int j = 0; j < this.freqOfAttr[i].length; ++j) {
                HashMapWritable<Text, IntWritable> value = new HashMapWritable<>();
                value.readFields(in);
                this.freqOfAttr[i][j] = value;
            }
        }

        numOfMembers.readFields(in);
    }

    public Float[][] getAttrSum(){
        FloatWritable[][] aS = (FloatWritable[][])attrSum.toArray();
        int k = aS.length;
        int n = aS[0].length;
        Float[][] clAtrSum = new Float[k][n];
        for (int l = 0; l < k; l++) {
            for (int j = 0; j < n; j++) {
                clAtrSum[l][j] = aS[l][j].get();
            }
        }
        return clAtrSum;
    }

    public Integer[] getNumOfMembers(){
        IntWritable[] n = (IntWritable[]) numOfMembers.toArray();
        Integer[] numOfM = new Integer[n.length];
        for (int i = 0; i < n.length; i++) {
            numOfM[i] = n[i].get();
        }
        return numOfM;
    }

    public HashMapWritable<Text, IntWritable>[][] getFreqOfCatAttr(){
        return freqOfAttr;
    }
}
