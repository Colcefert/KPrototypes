package com.company;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.io.FileReader;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.company.Utils.CentersReader;
import com.company.Utils.TwoDPointFileInputFormat;
import com.company.Utils.TwoDPointWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import static com.company.KMeans.KmeansMapper.centerfile;


public class KMeans {

    public static class KmeansMapper
            extends Mapper<LongWritable, TwoDPointWritable, IntWritable, TwoDPointWritable>{
        public static String centerfile="centers.txt";
        public ArrayList<Float> centroids = new ArrayList<>();

        public void setup(Context context) throws IOException {
            Scanner reader = new Scanner(new FileReader(centerfile));

            while (reader.hasNext()) {
                String s = reader.nextLine();
                String[] cs = s.split("\\s+");
                for(int i = 1; i < cs.length; i++) {
                    System.out.println(new Float(cs[i]));
                    centroids.add(new Float(cs[i]));
                }
            }
        }

        public void map(LongWritable key, TwoDPointWritable value, Context context
        ) throws IOException, InterruptedException {

            float distance=0;
            float mindistance=999999999.9f;
            int winnercentroid=-1;
            int i=0;
            Iterator<Float> it = centroids.iterator();
            while (it.hasNext()){
                float x_c = it.next();
                float y_c = it.next();
                ArrayList<FloatWritable> points = value.getData();
                float x = points.get(0).get();
                float y = points.get(1).get();

                distance = (x - x_c)*(x - x_c) + (y - y_c)*(y - y_c);
                if (distance < mindistance) {
                    mindistance = distance;
                    winnercentroid=i;
                }
                i++;
            }

            IntWritable winnerCentroid = new IntWritable(winnercentroid);
            context.write(winnerCentroid, value);
            System.out.printf("Map: Centroid = %d distance = %f\n", winnercentroid, mindistance);
        }
    }


    public static class KmeansReducer
            extends Reducer<IntWritable,TwoDPointWritable,IntWritable,Text> {

        public void reduce(IntWritable clusterid, Iterable<TwoDPointWritable> points,
                           Context context
        ) throws IOException, InterruptedException {

            int num = 0;
            float centerx=0.0f;
            float centery=0.0f;
            for (TwoDPointWritable point : points) {
                num++;
                ArrayList<FloatWritable> tpoints = point.getData();
                float x = tpoints.get(0).get();
                float y = tpoints.get(1).get();
                System.out.println(x + " " + y);
                centerx += x;
                centery += y;
            }
            centerx = centerx/num;
            centery = centery/num;

            String preres = String.format("%f %f", centerx, centery);
            Text result = new Text(preres);
            context.write(clusterid, result);
        }
    }


    public static void main(String[] args) throws Exception {
        String data = "/home/rvr/Documents/untitled2/inFiles/data.txt";
        String IN = "/home/rvr/Documents/untitled2/centers/centers.txt";
        String OUT = "/home/rvr/Documents/untitled2/outFiles/outFiles";
        String input = "";
        String output = OUT + System.nanoTime();

        int iteration = 0;
        boolean isdone = false;
        Job job = new Job(new Configuration(), "KMeans");
        while (isdone == false) {
            job = new Job(new Configuration(), "KMeans");
            if (iteration == 0) {
                Path toCache = new Path(IN);
                job.addCacheFile(toCache.toUri());
            } else {
                Path toCache = new Path(input + "/part-r-00000");
                job.addCacheFile(toCache.toUri());
                centerfile = "part-r-00000";
            }

            job.setJarByClass(KMeans.class);
            job.setMapperClass(KmeansMapper.class);
            job.setReducerClass(KmeansReducer.class);

            job.setInputFormatClass(TwoDPointFileInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(data));

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(TwoDPointWritable.class);

            FileOutputFormat.setOutputPath(job, new Path(output));
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            job.waitForCompletion(true);

            CentersReader r1 = new CentersReader();
            List<Float> centers_next = r1.read(output + "/part-r-00000");

            String prev;
            if (iteration == 0) {
                prev = IN;
            } else {
                prev = input + "/part-r-00000";
            }

            List<Float> centers_prev = r1.read(prev);

            Iterator<Float> it1 = centers_prev.iterator();
            Iterator<Float> it2 = centers_next.iterator();
            while (it2.hasNext()){
                float x1 = it1.next();
                float y1 = it1.next();
                float x2 = it2.next();
                float y2 = it2.next();
                if (Math.abs((x1 - x2)*(x1 - x2)+(y1 - y2)*(y1 - y2)) <= 0.01) {
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
