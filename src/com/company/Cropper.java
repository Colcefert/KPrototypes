package com.company;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;



/**
 * Created by rvr on 29.07.16.
 */
public class Cropper {
    int numberOfDO = 3428;
    int numberOfAttr = 21;
    int attrTotalNum = numberOfDO*numberOfAttr;
    int percents = 20;
    int tries =(int) (attrTotalNum * 0.01 * percents);


    public void read() {
        Path file = FileSystems.getDefault().getPath( "/home/rvr/Documents/untitled2/inFiles/ann-test.data");
        Charset charset = Charset.forName("US-ASCII");
        ArrayList<String[]> dataObjects = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(file, charset)) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                dataObjects.add(line.split("\t| "));
            }
        } catch (IOException x) {
            System.err.format("IOException: %s%n", x);
        }
        crop(dataObjects);

        Path file1 = FileSystems.getDefault().getPath( "/home/rvr/Documents/untitled2/inFiles/ann-test.data1");
        String result = "";
        for(String[] s: dataObjects) {
            for (int i = 0; i < s.length+1; i++) {
                if (i != s.length) {
                    result += s[i] + " ";
                } else {
                    result += "\n";
                }
            }
        }

        try (BufferedWriter writer = Files.newBufferedWriter(file1, charset)) {
            writer.write(result);
        } catch (IOException x) {
            System.err.format("IOException: %s%n", x);
        }

    }


     public void crop(ArrayList<String[]> dataObjects){
         while(tries > 0){
             int i = (int)((Math.random()%numberOfDO)*numberOfDO);
             //if(i > numberOfDO) {i = i/3000;}
             String[] dataObject = dataObjects.get(i);
             int j = (int)((Math.random()%numberOfAttr)*numberOfAttr);
              if(!"?".equals(dataObject[j])){
                 dataObject[j]="?";
                 tries--;
             } else {
                crop(dataObjects);
             }
         }
     }
}
