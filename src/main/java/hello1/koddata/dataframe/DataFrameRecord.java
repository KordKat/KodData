package hello1.koddata.dataframe;

import hello1.koddata.engine.Value;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataFrameRecord {

    private final String[] columns;
    private final Value<?>[] values;
    private Map<String, Value<?>> map;
    public DataFrameRecord(String[] columns, Value<?>[] values){
        this.columns = columns;
        this.values = values;
        map = new ConcurrentHashMap<>();
    }

    public String[] getColumns() {
        return columns;
    }

    public Value<?>[] getValues() {
        return values;
    }



}
