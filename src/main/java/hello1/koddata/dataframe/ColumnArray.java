package hello1.koddata.dataframe;

import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.memory.MemoryGroup;
import hello1.koddata.utils.Serializable;
import hello1.koddata.utils.collection.ImmutableArray;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ColumnArray implements Serializable {

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

    public ColumnArray distributeColumn(int startIdx, int endIdx) throws KException {
        // 1. ตรวจสอบว่ามี Column หรือไม่
        if (columns.isEmpty()) {
            return new ColumnArray(new ImmutableArray<>(new Column[0]), this.memoryGroup);
        }

        // 2. ตรวจสอบเงื่อนไขดัชนี
        int numRows = 0;
        // หาจำนวนแถวทั้งหมดจาก Column แรก
        Column firstColumn = columns.values().iterator().next();
        if (firstColumn != null) {
            numRows = firstColumn.getMetaData().getRows();
        }

        if (startIdx < 0 || endIdx > numRows || startIdx >= endIdx) {
            // อาจโยน Exception หรือคืนค่าว่าง/เดิม ขึ้นอยู่กับนโยบายของระบบ
            throw new KException(ExceptionCode.KD00006, "You cant use start < 0 , end > row , start = end");
        }

        // 3. สร้าง ImmutableArray สำหรับเก็บ Column ย่อย
        List<Column> distributedColumnsList = new ArrayList<>(columns.size());

        // 4. วนลูปผ่าน Column ทุกตัวใน ColumnArray ต้นฉบับ
        for (Column column : columns.values()) {

            // 5. เรียกใช้เมธอด distributeColumn ในคลาส Column เพื่อสร้าง Column ชิ้นใหม่
            // โดย Column ใหม่จะอ้างอิงไปยัง Memory เดิม แต่มี startIdx และ endIdx ใหม่
            Column distributedColumnShard = column.distributeColumn(startIdx, endIdx);

            // 6. เพิ่ม Column ย่อย (Shard) เข้าใน List
            distributedColumnsList.add(distributedColumnShard);
        }

        // 7. สร้าง ColumnArray ใหม่จาก List ของ Column ย่อย
        ImmutableArray<Column> distributedColumns = new ImmutableArray<>(distributedColumnsList.toArray(new Column[0]));

        // 8. คืนค่า ColumnArray ใหม่ (ชิ้นส่วน/Shard)
        // ใช้ MemoryGroup เดิม เนื่องจาก Column ย่อยยังคงอ้างอิง Memory เดิม
        return new ColumnArray(distributedColumns, this.memoryGroup);
    }


    @Override
    public byte[] serialize() throws KException {
        return new byte[0];
    }

    @Override
    public void deserialize(byte[] b) {

    }
}
