package com.company.Utils.IO.Readable;

import java.util.ArrayList;

public class DataObject {

    ArrayList<Float> numericAttr;
    ArrayList<String> catAttr;
    int classNumber;
    public DataObject(){
        numericAttr = new ArrayList<>();
        catAttr = new ArrayList<>();

    }

    public DataObject(ArrayList<Float> numericAttr, ArrayList<String> catAttr){
        this.numericAttr = numericAttr;
        this.catAttr = catAttr;
    }

    public DataObject(ArrayList<Float> numericAttr, ArrayList<String> catAttr, int classNumber){
        this.numericAttr = numericAttr;
        this.catAttr = catAttr;
        this.classNumber = classNumber;
    }

    public ArrayList<Float> getNumValues(){
        return numericAttr;
    }

    public ArrayList<String> getCatValues(){
        return catAttr;
    }


}
