package hello1.koddata.dataframe;

import java.util.Arrays;
import java.util.Map;

public class DataFrameSchema {

    int[] size;
    String[] keys;

    public DataFrameSchema(String[] keys, int[] size){
        this.keys = keys;
        this.size = size;
    }

    public DataFrameSchema(int initialSize){
        this(new String[initialSize], new int[initialSize]);
    }

    public DataFrameSchema(){
        this(20);
    }

    public void put(String key, int size){
        int exist = Arrays.binarySearch(keys, key);
        if(exist < 0) {
            //
        }
    }

    public boolean putIfAbsent(String key, int size){
        return false;
    }

    public int get(String key) { return -1; }

    public boolean contains(String key) {return false; }

    public DataFrameSchema add(String column, int size){
        this.putIfAbsent(column, size);
        return this;
    }

    public DataFrameSchema select(String...columns) { return null; }


}
