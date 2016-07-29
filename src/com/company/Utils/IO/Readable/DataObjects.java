package com.company.Utils.IO.Readable;

import java.util.ArrayList;

/**
 * Created by rvr on 12.07.16.
 */
public class DataObjects {

    ArrayList<DataObject> dataObjects;
    Float[] maxForAttr;
    Float[] minForAttr;

    public DataObjects(DataObject ... dO){
        for (DataObject data: dO) {
            dataObjects.add(data);
        }
    }

    public DataObjects(ArrayList<DataObject> dataObjects, Float[] maxForAttr, Float[] minForAttr){
        this.dataObjects = dataObjects;
        this.maxForAttr = maxForAttr;
        this.minForAttr = minForAttr;
    }

    public ArrayList<DataObject> getDataObjects(){
        return dataObjects;
    }

    public Float[] getMaxForEachAttr(){
        return maxForAttr;
    }

    public Float[] getMinForEachAttr(){
        return minForAttr;
    }
}
