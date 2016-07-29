package com.company;


import com.company.Utils.Distances;
import com.company.Utils.IO.Readable.DataObject;
import com.company.Utils.IO.Readable.DataObjects;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

import static com.company.KPrototypes.*;

public class CentersChoser {
    public static class KNNMapper extends Mapper<NullWritable, DataObjects, IntWritable, Text> {
        public void map(NullWritable key, DataObjects values, Context context
        ) throws IOException, InterruptedException {
            float theta = 0.02f;
            ArrayList<DataObject> dataObjects = values.getDataObjects();
            Float[] max = values.getMaxForEachAttr();
            Float[] min = values.getMinForEachAttr();

            for (int n = 0; n < k; n++) {
                LinkedList<Integer> centrAndNeighbors = new LinkedList<>();

                for (int i = 0; i < dataObjects.size(); i++) {
                    LinkedList<Integer> objAndNeighbors = new LinkedList<>();
                    DataObject dataObj1 = dataObjects.get(i);
                    objAndNeighbors.add(i);

                    for (int j = 0; j < dataObjects.size(); j++) {

                        if (i != j) {
                            DataObject dataObj2 = dataObjects.get(j);
                            if (Distances.computeMixedSetDissimilarity(dataObj1, dataObj2, max, min) <= theta) {
                                objAndNeighbors.add(j);
                            }
                        }
                    }

                    if (objAndNeighbors.size() > centrAndNeighbors.size()) {
                        centrAndNeighbors = objAndNeighbors;
                    }
                }
                DataObject center = dataObjects.get((centrAndNeighbors.get(1)));
                ArrayList<Float> numValues = center.getNumValues();
                ArrayList<String> catValues = center.getCatValues();
                String preres = "";
                for(int i = 0; i < numberOfNumericAttr; i++){
                    String iAttr = String.format("%f", numValues.get(i));
                    if(i == 0) preres += "" + iAttr;
                    else preres += " " + iAttr;
                }
                for(int i = 0; i < numberOfCatAttr; i++){
                    String val = catValues.get(i);
                    if(i==0) preres += " " + val + " ";
                    else preres += val + " ";
                }

                Text result = new Text(preres);
                context.write(new IntWritable(1), result);
                 centrAndNeighbors.size();
                for (int m = centrAndNeighbors.size() - 1; m > 0 ; m--){
                    dataObjects.remove((int) centrAndNeighbors.get(m));
                }

                dataObjects.trimToSize();
                theta += 0.05;
            }
        }
    }
}
