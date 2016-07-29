package com.company.Utils.IO.Readable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by rvr on 12.07.16.
 */
public class DataObjectsRecordReader extends RecordReader<NullWritable, DataObjects> {
        LineRecordReader lineReader;
        DataObjects value;
        boolean processed = false;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext attempt)
                throws IOException, InterruptedException {
            lineReader = new LineRecordReader();
            lineReader.initialize(inputSplit, attempt);

        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!processed) {
                ArrayList<DataObject> finalValue = new ArrayList<>();
                Float[] maxForAttr = new Float[0];
                Float[] minForAttr = new Float[0];
                int it = 0;
                int numberOFNumAttr = 0;
                Integer classNumber = 0;
                while (lineReader.nextKeyValue()) {
                    ArrayList<Float> numData = new ArrayList<>();
                    ArrayList<String> catData = new ArrayList<>();
                    String[] str = lineReader.getCurrentValue().toString().replace("\"", "").split(" ");
                    if(it == 0) {
                        readDO(str, numData, catData, classNumber);
                            numberOFNumAttr = numData.size();
                            maxForAttr = new Float[numberOFNumAttr];
                            minForAttr = new Float[numberOFNumAttr];
                            for (int i = 0; i < numData.size(); i++) {
                                Float attr_i = numData.get(i);
                                if (attr_i != null) {
                                    if (attr_i > -Float.MAX_VALUE) {
                                        maxForAttr[i] = attr_i;
                                    }
                                    if (attr_i < Float.MAX_VALUE) {
                                        minForAttr[i] = attr_i;
                                    }
                                } else {
                                    maxForAttr[i] = -Float.MAX_VALUE;
                                    minForAttr[i] = Float.MAX_VALUE;
                                }
                            }
                    } else {
                            String s = str[0];
                            if ("?".equals(s)) {
                                numData.add(null);
                            } else {
                                Float valueOfIAttr = Float.parseFloat(s);
                                numData.add(valueOfIAttr);
                                if (valueOfIAttr > maxForAttr[0]) {
                                    maxForAttr[0] = valueOfIAttr;
                                }
                                if (valueOfIAttr < minForAttr[0]) {
                                    minForAttr[0] = valueOfIAttr;
                                }
                            }
                            for (int i = 1; i < 16; i++) {
                                if (str[i].equals("?")) {
                                    catData.add(null);
                                } else {
                                    catData.add(str[i]);
                                }
                            }
                            int j = 1;
                            for (int i = 16; i < str.length -1; i++) {
                                if ("?".equals(str[i])) {
                                    numData.add(null);
                                } else {
                                    Float valueOfIAttr = Float.parseFloat(str[i]);
                                    numData.add(valueOfIAttr);
                                    if (valueOfIAttr > maxForAttr[j]) {
                                        maxForAttr[j] = valueOfIAttr;
                                    }
                                    if (valueOfIAttr < minForAttr[j]) {
                                        minForAttr[j] = valueOfIAttr;
                                    }
                                }
                                j++;
                            }
                        classNumber = Integer.parseInt(str[21]);
                    }

                    DataObject temp = new DataObject(numData, catData, classNumber);
                    finalValue.add(temp);
                    it++;
                }
                value = new DataObjects(finalValue, maxForAttr, minForAttr);
                lineReader.close();
                processed = true;
                return true;
            }
            return false;
        }

    public static void readDO(String[] str, ArrayList<Float> numData, ArrayList<String> catData, Integer classNumber) {
        String s = str[0];
        if ("?".equals(s)) {
            numData.add(null);
        } else {
            numData.add(Float.parseFloat(s));
        }
        for (int i = 1; i < 16; i++) {
            if ("?".equals(str[i])) {
                catData.add(null);
            } else {
                catData.add(str[i]);
            }
        }
        for (int i = 16; i < str.length -1; i++) {
            if ("?".equals(str[i])) {
                numData.add(null);
            } else {
                numData.add(Float.parseFloat(str[i]));
            }
        }

        //classNumber = Integer.parseInt(str[21]);
    }

    @Override
        public NullWritable getCurrentKey() throws IOException,
                InterruptedException {
            return NullWritable.get();
        }

        @Override
        public DataObjects getCurrentValue() throws IOException,
                InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return processed ? 1.0f : 0.0f;
        }

        @Override
        public void close() throws IOException {

        }


}
