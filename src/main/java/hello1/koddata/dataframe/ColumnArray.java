package hello1.koddata.dataframe;

import hello1.koddata.engine.Value;
import hello1.koddata.exception.KException;
import hello1.koddata.utils.collection.ImmutableArray;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ColumnArray {
    // Encapsulation
    private final Map<String, Column> columns = new LinkedHashMap<>();

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
    public Map<String, Column> getColumns() {
        return columns;
    }

    //    Polymorphism
    @Override
    public String toString() {
        if (columns.isEmpty()) return "<EMPTY>";

        List<String> names = new ArrayList<>(columns.keySet());
        int numRows = columns.get(names.get(0)).size();
        Map<String, Integer> columnWidths = new LinkedHashMap<>();
        for (String name : names) {
            Column col = columns.get(name);
            int maxWidth = name.length();

            for (int i = 0; i < numRows; i++) {
                String valStr = String.valueOf(col.readRow(i));
                if (valStr.length() > maxWidth) {
                    maxWidth = valStr.length();
                }
            }
            columnWidths.put(name, maxWidth + 2);
        }

        StringBuilder sb = new StringBuilder();

        for (String name : names) {
            int width = columnWidths.get(name);
            sb.append(String.format("%-" + width + "s", name));
        }
        sb.append("\n");

        for (String name : names) {
            int width = columnWidths.get(name);
            sb.append("-".repeat(width - 1)).append(" ");
        }
        sb.append("\n");

        for (int row = 0; row < numRows; row++) {
            for (String name : names) {
                Column c = columns.get(name);
                int width = columnWidths.get(name);
                sb.append(String.format("%-" + width + "s", c.readRow(row)));
            }
            sb.append("\n");
        }

        return sb.toString();
    }
}