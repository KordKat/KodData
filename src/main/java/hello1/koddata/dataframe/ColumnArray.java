package hello1.koddata.dataframe;

import hello1.koddata.engine.Value;
import hello1.koddata.exception.KException;
import hello1.koddata.utils.collection.ImmutableArray;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ColumnArray {
    private final ConcurrentMap<String, Column> columns = new ConcurrentHashMap<>();

    public ColumnArray(ImmutableArray<Column> columns){
        columns.forEach(x -> {
            this.columns.put(x.getMetaData().getName(), x);
        });
    }
    public void addColumn(Column column){
        columns.put(column.getMetaData().getName(), column);
    }
    public void removeColumn(String name){
        columns.remove(name);
    }
    public void deallocate(){
        columns.clear();
    }
    public boolean contains(String name){
        return columns.containsKey(name);
    }
    public DataFrameRecord[] toRecords() throws KException {
        if (columns.isEmpty()) {
            return new DataFrameRecord[0];
        }
        String[] columnNames = columns.keySet().toArray(new String[0]);
        Column[] allColumns = new Column[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            allColumns[i] = columns.get(columnNames[i]);
        }
        int numRows = allColumns[0].size();
        if (numRows == 0) {
            return new DataFrameRecord[0];
        }
        DataFrameRecord[] records = new DataFrameRecord[numRows];
        for (int i = 0; i < numRows; i++) {
            Value<?>[] rowValues = new Value<?>[columnNames.length];
            for (int j = 0; j < columnNames.length; j++) {
                Column currentColumn = allColumns[j];
                rowValues[j] = currentColumn.readRow(i);
            }
            records[i] = new DataFrameRecord(columnNames, rowValues);
        }
        return records;
    }
    public ConcurrentMap<String, Column> getColumns() {
        return columns;
    }
    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();
        for (Column column : columns.values()){
            res.append(column.toString()).append("\n");
        }
        return res.toString();
    }
}