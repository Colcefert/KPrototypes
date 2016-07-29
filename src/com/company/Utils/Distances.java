package com.company.Utils;

import com.company.Utils.IO.Readable.DataObject;

import java.util.ArrayList;

/**
 * Created by rvr on 19.07.16.
 */
public class Distances {

    public static Float computeMixedSetDissimilarity(DataObject dataObj1, DataObject dataObj2, Float [] maxForAttr, Float [] minForAttr){
        if (dataObj1.getCatValues() == null || dataObj2.getCatValues() == null) {
            return numericAttrDissim(dataObj1.getNumValues(), dataObj2.getNumValues(), maxForAttr, minForAttr);
        } else if(dataObj1.getNumValues()==null && dataObj2.getNumValues() == null){
            return categoricalAttrDissim(dataObj1.getCatValues(), dataObj2.getCatValues());
        }
        int wC = dataObj1.getCatValues().size();
        int wN = dataObj1.getNumValues().size();
        float numerator = wC*categoricalAttrDissim(dataObj1.getCatValues(), dataObj2.getCatValues())+wN*numericAttrDissim(dataObj1.getNumValues(), dataObj2.getNumValues(), maxForAttr, minForAttr);
        float denom = wC + wN;
        return numerator/denom;
    }

    private static float categoricalAttrDissim(ArrayList<String> obj1Cat, ArrayList<String> obj2Cat){
        float distance = 0;

        for (int i = 0; i < obj1Cat.size(); i++){
            float res = 0;
            String obj1Attr = obj1Cat.get(i);
            String obj2Attr = obj2Cat.get(i);
            if(obj1Attr != null && obj2Attr != null && !obj1Attr.equals(obj2Attr)){
                res=1;
            }
            distance += res;
        }

        float denominator = obj1Cat.size() - distance;
        return distance/denominator;
    }

    public static float numericAttrDissim(ArrayList<Float> dataObjectNumerics, ArrayList<Float> centroidNumerics, Float [] maxForAttr, Float [] minForAttr){
        float distance = 0;
        for (int i = 0; i < dataObjectNumerics.size(); i++){
            float res = 0;
            if (dataObjectNumerics.get(i) != null && centroidNumerics.get(i) != null) {
                float numerator = Math.abs(dataObjectNumerics.get(i) - centroidNumerics.get(i));
                float denom = maxForAttr[i] - minForAttr[i];
                res = numerator/denom;
                distance += res*res;
            }
        }
        distance = (float) Math.sqrt(distance);
        float denominator = dataObjectNumerics.size() - distance;
        return distance = distance/denominator;
    }
}
