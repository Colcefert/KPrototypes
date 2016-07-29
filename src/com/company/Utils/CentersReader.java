package com.company.Utils;

import com.company.Utils.IO.Readable.DataObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;


public class CentersReader {

    public ArrayList<DataObject> read(String path) throws Exception{
        Path ofile = new Path(path);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(
                fs.open(ofile)));
        ArrayList<DataObject> centers = new ArrayList<>();
        String line = br.readLine();

        while (line != null) {
            ArrayList<Float> num = new ArrayList<>();
            ArrayList<String> cat = new ArrayList<>();
            String[] str = line.replace("\"", "").split("\t| ");
            read(str, num, cat);
            centers.add(new DataObject(num,cat));
            line = br.readLine();
        }
        br.close();
        return centers;
    }

    public void read(String[] str, ArrayList<Float> num, ArrayList<String> cat){

        for (int i = 0; i < 6; i++) {
            if ("null".equals(str[i])) {
                num.add(null);
            } else {
                num.add(Float.parseFloat(str[i]));
            }
        }

        for (int i = 6; i < str.length ; i++) {
            if ("null".equals(str[i])) {
                cat.add(null);
            } else {
                cat.add(str[i]);
            }
        }
    }
}
