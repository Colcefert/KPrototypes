package com.company;

import com.company.Utils.CentersReader;
import com.company.Utils.Distances;
import com.company.Utils.IO.Readable.DataObject;
import com.company.Utils.IO.Readable.DataObjects;
import com.company.Utils.IO.Readable.DataObjectsFileInputFormat;
import com.company.Utils.IO.Writable.MapOutput.ClustersMetrics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static com.company.KPrototypes.KpMapper.centerfile;


public class KPrototypes {
    static int k = 3;
    static int numberOfNumericAttr = 6;
    static int numberOfCatAttr = 15;
    static Integer[] membership= new Integer[3428];
    static Float[][] clustAttrSum = new Float[k][numberOfNumericAttr];
    static int[] numOfClusterMembers = new int[k];
    static HashMap<Text, IntWritable>[][] freqOfCatAttr = (HashMap<Text, IntWritable>[][]) new HashMap<?,?>[k][numberOfCatAttr];
    static Float [] maxForAttr;
    static Float [] minForAttr;

    public static class KpMapper extends Mapper<NullWritable, DataObjects, IntWritable, ClustersMetrics> {

        ArrayList<DataObject> centroids = new ArrayList<>();
        static String centerfile = "part-r-00000";

        public void setup(Context context) throws IOException {
             Scanner reader = new Scanner(new FileReader(centerfile));

            while (reader.hasNext()) {                                                  //reading centers
                String line = reader.nextLine();
                String[] str1 = line.replace("\"", "").split("\t| ");
                String[] str = Arrays.copyOfRange(str1, 1, str1.length);
                ArrayList<Float> numericAttr = new ArrayList<>();
                ArrayList<String> catAttr = new ArrayList<>();
                CentersReader centersR = new CentersReader();
                centersR.read(str, numericAttr, catAttr);

                DataObject center = new DataObject(numericAttr, catAttr);
                centroids.add(center);
            }
        }

        public void map(NullWritable key, DataObjects values, Context context
        ) throws IOException, InterruptedException {

            float distance = 0;
            float mindistance = 999999999.9f;
            Integer newCluster = -1;
            Integer oldCluster = -1;
            int nOfClust = 0;

            ArrayList<DataObject> dataObjects = values.getDataObjects();
            maxForAttr = values.getMaxForEachAttr();
            minForAttr = values.getMinForEachAttr();

            for (int n = 0; n < dataObjects.size(); n++) {                          // for each object in DataSet computing distance to each centroid
                DataObject dataObject = dataObjects.get(n);

                ArrayList<Float> num = dataObject.getNumValues();
                ArrayList<String> cat = dataObject.getCatValues();

                for (DataObject centroid : centroids) {
                        distance = Distances.computeMixedSetDissimilarity(dataObject, centroid, maxForAttr, minForAttr);
                        if (distance < mindistance) {                               // adding object to nearest cluster
                            mindistance = distance;
                            newCluster = nOfClust;
                        }
                    nOfClust++;
                }
                nOfClust = 0;
                mindistance = 9999999999999f;
                oldCluster = membership[n];

                if(oldCluster==null){                                                   //if it first iteration

                    for (int j = 0; j < numberOfNumericAttr; j++) {
                        Float atrValue = num.get(j);
                        if(atrValue != null) {
                            if (clustAttrSum[newCluster][j] == null) {
                                clustAttrSum[newCluster][j] = atrValue;
                            } else {
                                clustAttrSum[newCluster][j] = clustAttrSum[newCluster][j] + atrValue;
                            }
                        }
                    }

                    for(int j = 0; j < numberOfCatAttr; j++){
                        String catAttr = cat.get(j);
                        if(catAttr != null){
                            if(freqOfCatAttr[newCluster][j]!=null && freqOfCatAttr[newCluster][j].containsKey(new Text(catAttr))){
                                int temp = freqOfCatAttr[newCluster][j].get(new Text(catAttr)).get();
                                freqOfCatAttr[newCluster][j].put(new Text(catAttr), new IntWritable(temp+1));
                            } else {
                                HashMap<Text, IntWritable> jCatAttr = new HashMap<>();
                                jCatAttr.put(new Text(catAttr), new IntWritable(1));
                                freqOfCatAttr[newCluster][j] = jCatAttr;
                            }
                        }
                    }
                    numOfClusterMembers[newCluster] += 1;
                    membership[n]= newCluster;
                }  else if (newCluster != oldCluster) {
                    for (int j = 0; j < numberOfNumericAttr; j++) {
                        Float atrValue = num.get(j);
                        if(atrValue != null) {
                            clustAttrSum[newCluster][j] += atrValue;
                            clustAttrSum[newCluster][j] -= atrValue;
                        }
                    }

                    for(int j = 0; j < numberOfCatAttr; j++){
                        String catAttr = cat.get(j);
                        Text k = new Text(catAttr);
                        if(freqOfCatAttr[newCluster][j].containsKey(k)){
                            int temp = freqOfCatAttr[newCluster][j].get(k).get();
                            freqOfCatAttr[newCluster][j].put(k, new IntWritable(temp+1));
                        } else {
                            freqOfCatAttr[newCluster][j].put(k, new IntWritable(1));
                        }
                        if (freqOfCatAttr[oldCluster][j].containsKey(k)){
                            int temp = freqOfCatAttr[oldCluster][j].get(k).get();
                            freqOfCatAttr[oldCluster][j].put(k, new IntWritable(temp-1));
                        }
                    }
                    numOfClusterMembers[newCluster] += 1;
                    numOfClusterMembers[oldCluster] -= 1;
                    membership[n]= newCluster;
                }
            }
            ClustersMetrics clusterMetrics = new ClustersMetrics(clustAttrSum, freqOfCatAttr, numOfClusterMembers);
            context.write(new
                    IntWritable(1), clusterMetrics);
        }
    }

    public static class KpReducer
            extends Reducer<IntWritable, ClustersMetrics,IntWritable,Text> {

        public void reduce(IntWritable clusterid, Iterable<ClustersMetrics> clusterMetrics,
                           Context context
        ) throws IOException, InterruptedException {
            DataObject[] newCenter = new DataObject[k];
            Float[][] clusterAttrSum = new Float[k][numberOfNumericAttr];
            Integer[] numberOfClusterMembers = new Integer[k];
            HashMap<Text, IntWritable>[][] freqOfCategoricalAttr = (HashMap<Text, IntWritable>[][]) new HashMap<?,?>[k][numberOfCatAttr];

            for (ClustersMetrics cM : clusterMetrics) {
                Float[][] cAS = cM.getAttrSum();
                Integer[] nOfM = cM.getNumOfMembers();
                HashMap<Text, IntWritable>[][] freqOfCatAttr = cM.getFreqOfCatAttr();
                for(int i = 0; i< k; i++){
                    for (int j = 0; j < numberOfNumericAttr; j++) {
                        if(clusterAttrSum[i][j] == null) clusterAttrSum[i][j] = cAS[i][j];
                        else clusterAttrSum[i][j] += cAS[i][j];
                    }

                    for(int j = 0; j < numberOfCatAttr; j++) {
                        HashMap<Text, IntWritable> currentAttrFreq = freqOfCategoricalAttr[i][j];
                        if (currentAttrFreq==null) {
                            currentAttrFreq = new HashMap<>();
                        }

                        Iterator<Map.Entry<Text, IntWritable>> itr = freqOfCatAttr[i][j].entrySet().iterator();
                        while (itr.hasNext()) {
                            Map.Entry<Text, IntWritable> curValFreq = itr.next();
                            Text key = curValFreq.getKey();
                            IntWritable value = curValFreq.getValue();
                            if (currentAttrFreq.containsKey(key)) {
                                int newFreq = currentAttrFreq.get(key).get() + value.get();
                                currentAttrFreq.put(key, new IntWritable(newFreq));
                            } else {
                                currentAttrFreq.put(key, value);
                            }
                        }

                        freqOfCategoricalAttr[i][j] = currentAttrFreq;
                    }
                    if(numberOfClusterMembers[i] == null) numberOfClusterMembers[i] = nOfM[i];
                    else numberOfClusterMembers[i] += nOfM[i];
                }
            }

            for(int i = 0; i < k; i++){
                ArrayList<Float> newCenterNum = new ArrayList<>();
                ArrayList<String> newCenterCat = new ArrayList<>();
                float denom = numberOfClusterMembers[i];
                String preres = new String();
                for (int j = 0; j < numberOfNumericAttr; j++) {
                    newCenterNum.add(clusterAttrSum[i][j]/denom);
                    String jAttr = String.format("%f", newCenterNum.get(j));
                    if(j == 0) preres += ""+ jAttr;
                    else preres += " " + jAttr;
                }

                for(int j = 0; j < numberOfCatAttr; j++){
                    String val = highestFreq(freqOfCategoricalAttr[i][j]);
                    newCenterCat.add(val);
                    if(j==0) preres += " " + val + " ";
                    else preres += val + " ";
                }

                newCenter[i] = new DataObject(newCenterNum, newCenterCat);
                Text result = new Text(preres);
                context.write(clusterid, result);
            }
        }
        
        public String highestFreq(HashMap<Text, IntWritable> freqOfJAttrValues){
            Iterator<Map.Entry<Text, IntWritable>> itr = freqOfJAttrValues.entrySet().iterator();
            int max = 0;
            String attr ="";
            while (itr.hasNext()) {
                Map.Entry <Text, IntWritable> temp =  itr.next();
                if(temp.getValue().get() > max){
                    max = temp.getValue().get();
                    attr = temp.getKey().toString();
                }
            }
            return attr;
        }
    }


    public static void main(String[] args) throws Exception {
        String data = "/home/rvr/Documents/untitled2/inFiles/ann-test.data1";
        String IN = "/home/rvr/Documents/untitled2/outFiles/initial_centers/part-r-00000";
        String OUT = "/home/rvr/Documents/untitled2/outFiles/outFiles/centers";
        String input = "";
        String output = OUT + System.nanoTime();

        int iteration = 0;
        boolean isdone = false;

        Job job = new Job(new Configuration(), "CentersChoice");
        job.setJarByClass(CentersChoser.class);
        job.setMapperClass(CentersChoser.KNNMapper.class);
        job.setInputFormatClass(DataObjectsFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(data));
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path("/home/rvr/Documents/untitled2/outFiles/initial_centers/"));
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

        while (!isdone||iteration == 100) {
            job = new Job(new Configuration(), "KPrototypes");
            if (iteration == 0) {
                Path toCache = new Path(IN);
                job.addCacheFile(toCache.toUri());
            } else {
                Path toCache = new Path(input + "/part-r-00000");
                job.addCacheFile(toCache.toUri());
                centerfile = "part-r-00000";
            }

            job.setJarByClass(KPrototypes.class);
            job.setMapperClass(KpMapper.class);
            job.setReducerClass(KpReducer.class);

            job.setInputFormatClass(DataObjectsFileInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(data));

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(ClustersMetrics.class);

            FileOutputFormat.setOutputPath(job, new Path(output));
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            job.waitForCompletion(true);

            CentersReader r1 = new CentersReader();
            List<DataObject> centers_next = r1.read(output + "/part-r-00000");

            String prev;
            if (iteration == 0) {
                prev = IN;
            } else {
                prev = input + "/part-r-00000";
            }

            List<DataObject> centers_prev = r1.read(prev);

            Iterator<DataObject> it1 = centers_prev.iterator();
            Iterator<DataObject> it2 = centers_next.iterator();
            while (it2.hasNext()){
                DataObject prevCenter = it1.next();
                DataObject nextCenter = it2.next();
                float distance = Distances.computeMixedSetDissimilarity(prevCenter, nextCenter, maxForAttr, minForAttr);
                if (Math.abs(distance) <= 0.01) {
                    isdone = true;
                } else {
                    isdone = false;
                    break;
                }
            }
            ++iteration;
            input = output;
            output = OUT + System.nanoTime();
        }
    }
}
