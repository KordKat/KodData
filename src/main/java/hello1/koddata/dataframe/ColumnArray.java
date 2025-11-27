package hello1.koddata.dataframe;

import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.memory.MemoryGroup;
import hello1.koddata.utils.collection.ImmutableArray;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ColumnArray  {

    private ConcurrentMap<String, Column> columns = new ConcurrentHashMap<>();

    private MemoryGroup memoryGroup;


    private Set<InetSocketAddress> selectedNodes;

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

    public DataFrameRecord[] toRecords() throws KException {
        // 1. ตรวจสอบว่ามี Column หรือไม่
        if (columns.isEmpty()) {
            return new DataFrameRecord[0];
        }

        // 2. ดึงชื่อ Column และ Object Column
        String[] columnNames = columns.keySet().toArray(new String[0]);
        Column[] allColumns = new Column[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            allColumns[i] = columns.get(columnNames[i]);
        }

        // 3. หาจำนวนแถว (rows)
        // สมมติว่าทุก Column มีจำนวนแถวเท่ากัน, ใช้ Column แรกเป็นตัวอ้างอิง
        int numRows = allColumns[0].getMetaData().getRows();

        if (numRows == 0) {
            return new DataFrameRecord[0];
        }

        // 4. สร้าง Array สำหรับเก็บ DataFrameRecord
        DataFrameRecord[] records = new DataFrameRecord[numRows];

        // 5. สร้างและตั้งค่าเริ่มต้น DataFrameCursor สำหรับแต่ละ Column
        // ใน Column-major storage, แต่ละ Column จะมี Cursor เป็นของตัวเอง
        // Memory Layout: [Null Bitmap][Data]
        ConcurrentMap<String, DataFrameCursor> cursors = new ConcurrentHashMap<>();

        for (Column column : allColumns) {
            // คำนวณขนาด Null Bitmap: (rows + 7) / 8
            int rows = column.getMetaData().getRows();
            int nullBitmapSize = (rows + 7) / 8;

            DataFrameCursor cursor = new DataFrameCursor();

            // ตั้งค่า Cursor ให้เริ่มที่ตำแหน่งเริ่มต้นของ Data (หลัง Null Bitmap)
            cursor.setCursor(nullBitmapSize);

            cursors.put(column.getMetaData().getName(), cursor);
        }

        // 6. วนลูปตามจำนวนแถว (Rows)
        for (int i = 0; i < numRows; i++) {
            Value<?>[] rowValues = new Value<?>[columnNames.length];

            // 7. วนลูปตาม Column เพื่ออ่านค่าของแต่ละ Field ในแถวที่ i
            for (int j = 0; j < columnNames.length; j++) {
                Column currentColumn = allColumns[j];
                DataFrameCursor cursor = cursors.get(columnNames[j]);

                // ใช้เมธอด readRow(int index, DataFrameCursor dataFrameCursor)
                // index คือดัชนีของแถว (i)
                // cursor จะถูกปรับปรุงโดยเมธอด readRow ให้ชี้ไปยังตำแหน่งเริ่มต้นของค่าถัดไปโดยอัตโนมัติ
                rowValues[j] = currentColumn.readRow(i, cursor);
            }

            // 8. สร้าง DataFrameRecord สำหรับแถวนี้
            records[i] = new DataFrameRecord(columnNames, rowValues);
        }

        return records;
    }

    public ConcurrentMap<String, Column> getColumns() {
        return columns;
    }
}
