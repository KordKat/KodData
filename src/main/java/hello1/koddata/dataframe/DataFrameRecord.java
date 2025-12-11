package hello1.koddata.dataframe;

import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataFrameRecord {
    // Encapsulation
    private final String[] columns;
    private final Value<?>[] values;
    private Map<String, Value<?>> map;



    public DataFrameRecord(String[] columns, Value<?>[] values) throws KException{
        if(columns.length != values.length) {
            throw new KException(ExceptionCode.KDD0008, "number of column should be equals to number of values");
        }
        this.columns = columns;
        this.values = values;
        map = new ConcurrentHashMap<>();
        for(int i = 0; i < columns.length; i++){
            map.put(columns[i], values[i]);
        }
    }

    //Encapsulation
    public String[] getColumns() {
        return columns;
    }

    //Encapsulation
    public Value<?>[] getValues() {
        return values;
    }

    //Encapsulation
    public Value<?> get(String column){
        return map.get(column);
    }


}
