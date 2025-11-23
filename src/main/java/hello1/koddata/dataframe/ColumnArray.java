package hello1.koddata.dataframe;

import hello1.koddata.memory.MemoryGroup;
import hello1.koddata.utils.collection.ImmutableArray;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ColumnArray {

    private ConcurrentMap<String, Column> columns = new ConcurrentHashMap<>();

    private MemoryGroup memoryGroup;

    public ColumnArray(ImmutableArray<Column> columns, MemoryGroup memoryGroup){
        this.memoryGroup = memoryGroup;
        columns.forEach(x -> {
            this.columns.put(x.getMetaData().getName(), x);
        });
    }

    public void addColumn(Column column){
        if(columns.containsKey(column.getMetaData().getName())){
            Column old = columns.get(column.getMetaData().getName());
            memoryGroup.deallocate(old.getMemory());
            columns.put(column.getMetaData().getName(), column);
        }
    }

    public void removeColumn(String name){
        if(columns.containsKey(name)){
            Column column = columns.get(name);
            memoryGroup.deallocate(column.getMemory());
            columns.remove(name);
        }
    }

    public void deallocate(){
        for (String s : columns.keySet()){
            removeColumn(s);
        }
    }

    public boolean contains(String name){
        return columns.containsKey(name);
    }

    public DataFrameRecord[] toRecords(){}
}
