package com.company.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;


public class CentersReader {

    public ArrayList<Float> read(String path) throws Exception{
        Path ofile = new Path(path);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(
                fs.open(ofile)));
        ArrayList<Float> centers = new ArrayList<Float>();
        String line = br.readLine();
        while (line != null) {
            String[] sp = line.split("\t| ");
            float c = Float.parseFloat(sp[1]);
            float d = Float.parseFloat(sp[2]);
            centers.add(c);
            centers.add(d);
            line = br.readLine();
        }
        br.close();
        return centers;
    }
}
