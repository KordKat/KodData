package hello1.koddata.concurrent;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.ColumnArray;
import hello1.koddata.dataframe.ColumnMetaData;
import hello1.koddata.dataframe.DataFrameCursor;
import hello1.koddata.dataframe.VariableElement;
import hello1.koddata.engine.*;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.Session;
import hello1.koddata.utils.collection.ImmutableArray;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class KTask implements Supplier<Value<?>> {

    private final QueryExecution execution;
    private boolean isCancelled = false;
    private Session session;
    private ColumnArray columnArray;

    public KTask(QueryExecution execution, ColumnArray columnArray, Session session) {
        this.execution = execution;
        this.columnArray = columnArray;
        this.session = session;
    }

    @Override
    public Value<?> get() {
        // ... (ส่วน Logic การประมวลผล while loop ยังคงเหมือนเดิม) ...

        // Mockup ส่วนต้นเพื่อความสมบูรณ์ของ Context
        Map<String, DataFrameCursor> cursorMap = columnArray.getColumns().keySet().stream().collect(Collectors.toMap(Function.identity(), k -> new DataFrameCursor()));
        int index = 0;
        int rows = columnArray.getColumns().values().stream().findAny().get().getMetaData().getRows();
        List<Value<?>> bufferValue = new ArrayList<>();
        Map<Integer, QueryOperationNode> barrier = new HashMap<>();
        Map<String, Value<?>> columnOperationResultMap = new HashMap<>();
        Map<String, List<Value<?>>> finalResult = columnArray.getColumns().keySet().stream().collect(Collectors.toMap(Function.identity(), x -> new ArrayList<>()));

        while(index < rows) {



            if(barrier.containsKey(index)) {

                QueryOperationNode barrierNode = barrier.get(index);

                ColumnOperation co = (ColumnOperation) barrierNode.getOperation();



                try {

                    Value<?> columnResult = co.operate(new Value<>(bufferValue));



                    bufferValue.clear();

                    barrier.remove(index);



                    columnOperationResultMap.put(barrierNode.getColumn(), columnResult);



                    QueryOperationNode next = barrierNode.getNextNode();



                    if (next != null && next.getOperation() instanceof ColumnOperation) {

                        bufferValue.add(columnResult);

                        barrier.put(index + 1, next);

                        index++;

                        continue;

                    } else if (next != null) {

                        // The flow will continue below with the standard loop

                    } else {

                        index++;

                        continue;

                    }

                } catch (KException e) {

                    throw new RuntimeException(e);

                }

            }



            QueryOperationNode next = execution.getHead().getNextNode();



            int finalIndex = index;

            Map<String, Value<?>> valueMap = columnArray.getColumns().keySet().stream()

                    .collect(Collectors.toMap(Function.identity(),

                            k-> columnArray.getColumns().get(k).readRow(finalIndex,cursorMap.get(k))));



            if (!columnOperationResultMap.isEmpty()) {

                valueMap.putAll(columnOperationResultMap);

                columnOperationResultMap.clear();

            }



            while (next != null) {



                QueryOperation op = next.getOperation();

                if (op instanceof ColumnOperation co) {



                } else {

                    try {

                        String reqColumn = next.getColumn();

                        Value<?> value = valueMap.get(reqColumn);

                        if(value == null){

                            if (columnOperationResultMap.containsKey(reqColumn)) {

                                value = columnOperationResultMap.get(reqColumn);

                            } else {

                                throw new KException(ExceptionCode.KD00005, "Dataframe does not have column, " + reqColumn);

                            }

                        }



                        Value<?> result = op.operate(value);



                        if(result == null) break;



                        if(next.getNextNode() != null && next.getNextNode().getOperation() instanceof ColumnOperation){

                            bufferValue.add(result);

                            barrier.put(index, next.getNextNode());

                            break;

                        } else if (next.getNextNode() == null) {

                            bufferValue.add(result);

                            break;

                        }



                        valueMap.put(reqColumn, result);



                    } catch (KException e) {

                        throw new RuntimeException(e);

                    }

                }

                next = next.getNextNode();

            }

            for(String s : columnArray.getColumns().keySet()){

                finalResult.get(s).add(valueMap.get(s));

            }

            index++;

        }

        // --- ส่วนที่ปรับปรุง: Infer Type ใหม่ และสร้าง Column ---

        Map<String, Column> newColumns = new HashMap<>();
        int resultRowCount = finalResult.values().stream().findFirst().map(List::size).orElse(0);

        for (String colName : finalResult.keySet()) {
            List<Value<?>> values = finalResult.get(colName);

            // ใช้ฟังก์ชัน inferType แทนการดึงจาก Metadata เดิม
            ColumnMetaData.ColumnDType type = inferType(values);

            try {
                Column newCol = switch (type) {
                    // Allocator 1: Scalar Fixed
                    case SCALAR_INT, SCALAR_DOUBLE, SCALAR_LOGICAL, SCALAR_DATE, SCALAR_TIMESTAMP
                            -> buildScalarFixedColumn(colName, values, type, resultRowCount);

                    // Allocator 2: Scalar String
                    case SCALAR_STRING
                            -> buildScalarStringColumn(colName, values, resultRowCount);

                    // Allocator 3: List Fixed
                    case LIST_INT, LIST_DOUBLE, LIST_LOGICAL, LIST_DATE, LIST_TIMESTAMP
                            -> buildListFixedColumn(colName, values, type, resultRowCount);

                    // Allocator 4: List String
                    case LIST_STRING
                            -> buildListStringColumn(colName, values, resultRowCount);
                };
                newColumns.put(colName, newCol);
            } catch (Exception e) {
                throw new RuntimeException("Error building column " + colName + " with inferred type " + type, e);
            }
        }

        return new Value<>(new ColumnArray(new ImmutableArray<>(newColumns.values().stream().toList()), session.getSessionData().getMemoryGroup()));
    }

    // --- Type Inference Logic ---

    private ColumnMetaData.ColumnDType inferType(List<Value<?>> values) {
        // 1. หาข้อมูลตัวแรกที่ไม่ใช่ Null เพื่อดู Class ของมัน
        Object sample = values.stream()
                .map(Value::get)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);

        // 2. ถ้าเป็น Null ทั้งหมด ให้ Default เป็น String (หรือ Type ที่ปลอดภัยที่สุด)
        if (sample == null) {
            return ColumnMetaData.ColumnDType.SCALAR_STRING;
        }

        // 3. ตรวจสอบ Scalar Type
        if (sample instanceof Integer) return ColumnMetaData.ColumnDType.SCALAR_INT;
        if (sample instanceof Double) return ColumnMetaData.ColumnDType.SCALAR_DOUBLE;
        if (sample instanceof Boolean) return ColumnMetaData.ColumnDType.SCALAR_LOGICAL;
        if (sample instanceof String) return ColumnMetaData.ColumnDType.SCALAR_STRING;
        // หมายเหตุ: ใน CSVLoader วันที่/เวลา ถูกเก็บเป็น Long.
        // ถ้าไม่สามารถแยก Date กับ Timestamp ได้ชัดเจน ปกติจะเลือก Timestamp เพื่อความละเอียดสูงสุด
        if (sample instanceof Long) return ColumnMetaData.ColumnDType.SCALAR_TIMESTAMP;

        // 4. ตรวจสอบ List Type
        if (sample instanceof List) {
            List<?> list = (List<?>) sample;
            if (list.isEmpty()) {
                // List ว่างเปล่า ไม่รู้ไส้ใน Default เป็น List String
                return ColumnMetaData.ColumnDType.LIST_STRING;
            }

            // ดูไส้ในของ List ตัวแรก
            Object inner = list.stream().filter(Objects::nonNull).findFirst().orElse(null);
            if (inner == null) return ColumnMetaData.ColumnDType.LIST_STRING;

            if (inner instanceof Integer) return ColumnMetaData.ColumnDType.LIST_INT;
            if (inner instanceof Double) return ColumnMetaData.ColumnDType.LIST_DOUBLE;
            if (inner instanceof Boolean) return ColumnMetaData.ColumnDType.LIST_LOGICAL;
            if (inner instanceof String) return ColumnMetaData.ColumnDType.LIST_STRING;
            if (inner instanceof Long) return ColumnMetaData.ColumnDType.LIST_TIMESTAMP;
        }

        // Fallback
        return ColumnMetaData.ColumnDType.SCALAR_STRING;
    }

    // --- Allocators (เหมือนเดิม แต่เรียกใช้โดย Logic ใหม่) ---

    private Column buildScalarFixedColumn(String name, List<Value<?>> values, ColumnMetaData.ColumnDType type, int rowCount) throws KException {
        int elementSize = switch (type) {
            case SCALAR_INT -> Integer.BYTES;
            case SCALAR_DOUBLE, SCALAR_DATE, SCALAR_TIMESTAMP -> Long.BYTES;
            case SCALAR_LOGICAL -> 1;
            default -> throw new IllegalArgumentException("Unsupported scalar fixed type: " + type);
        };

        ByteBuffer buffer = ByteBuffer.allocate(rowCount * elementSize);
        boolean[] flags = new boolean[rowCount];

        for (int i = 0; i < rowCount; i++) {
            Object v = values.get(i).get();
            if (v != null) {
                flags[i] = true;
                switch (type) {
                    case SCALAR_INT -> buffer.putInt(((Number) v).intValue()); // Cast Number เพื่อความปลอดภัย
                    case SCALAR_DOUBLE -> buffer.putDouble(((Number) v).doubleValue());
                    case SCALAR_LOGICAL -> buffer.put((byte) (((Boolean) v) ? 1 : 0));
                    case SCALAR_DATE, SCALAR_TIMESTAMP -> buffer.putLong(((Number) v).longValue());
                }
            } else {
                buffer.position(buffer.position() + elementSize);
            }
        }
        buffer.flip();
        return new Column(name, elementSize, session.getSessionData().getMemoryGroup().getName(), buffer, flags, elementSize, 0, rowCount, type);
    }

    private Column buildScalarStringColumn(String name, List<Value<?>> values, int rowCount) throws KException {
        List<VariableElement> list = new ArrayList<>(rowCount);
        boolean[] flags = new boolean[rowCount];

        for (int i = 0; i < rowCount; i++) {
            Object v = values.get(i).get();
            if (v != null) {
                flags[i] = true;
                list.add(VariableElement.newStringElement(v.toString()));
            } else {
                list.add(VariableElement.newElement(new byte[0]));
            }
        }
        return new Column(name, list, session.getSessionData().getMemoryGroup().getName(), flags, 0, rowCount, ColumnMetaData.ColumnDType.SCALAR_STRING);
    }

    @SuppressWarnings("unchecked")
    private Column buildListFixedColumn(String name, List<Value<?>> values, ColumnMetaData.ColumnDType type, int rowCount) throws KException {
        int elementSize = switch (type) {
            case LIST_INT -> Integer.BYTES;
            case LIST_DOUBLE, LIST_DATE, LIST_TIMESTAMP -> Long.BYTES;
            case LIST_LOGICAL -> 1;
            default -> throw new IllegalArgumentException("Unsupported list fixed type");
        };

        List<List<byte[]>> lists = new ArrayList<>(rowCount);
        List<boolean[]> perFlags = new ArrayList<>(rowCount);
        boolean[] colFlags = new boolean[rowCount];

        for (int i = 0; i < rowCount; i++) {
            Object v = values.get(i).get();
            if (v == null) {
                colFlags[i] = false;
                lists.add(List.of());
                perFlags.add(new boolean[0]);
                continue;
            }

            colFlags[i] = true;
            List<?> rawList = (List<?>) v;
            List<byte[]> rowBytes = new ArrayList<>(rawList.size());
            boolean[] rowFlag = new boolean[rawList.size()];

            for (int j = 0; j < rawList.size(); j++) {
                Object item = rawList.get(j);
                if (item == null) {
                    rowFlag[j] = false;
                    rowBytes.add(new byte[elementSize]);
                } else {
                    rowFlag[j] = true;
                    ByteBuffer bb = ByteBuffer.allocate(elementSize);
                    switch (type) {
                        case LIST_INT -> bb.putInt(((Number) item).intValue());
                        case LIST_DOUBLE -> bb.putDouble(((Number) item).doubleValue());
                        case LIST_LOGICAL -> bb.put((byte)(((Boolean) item) ? 1 : 0));
                        case LIST_DATE, LIST_TIMESTAMP -> bb.putLong(((Number) item).longValue());
                    }
                    rowBytes.add(bb.array());
                }
            }
            lists.add(rowBytes);
            perFlags.add(rowFlag);
        }
        return new Column(name, session.getSessionData().getMemoryGroup().getName(), lists, perFlags, colFlags, elementSize, 0, rowCount, type);
    }

    @SuppressWarnings("unchecked")
    private Column buildListStringColumn(String name, List<Value<?>> values, int rowCount) throws KException {
        List<List<VariableElement>> lists = new ArrayList<>(rowCount);
        List<boolean[]> perFlags = new ArrayList<>(rowCount);
        boolean[] colFlags = new boolean[rowCount];

        for (int i = 0; i < rowCount; i++) {
            Object v = values.get(i).get();
            if (v == null) {
                colFlags[i] = false;
                lists.add(List.of());
                perFlags.add(new boolean[0]);
                continue;
            }

            colFlags[i] = true;
            List<?> rawList = (List<?>) v;
            List<VariableElement> rowElements = new ArrayList<>(rawList.size());
            boolean[] rowFlag = new boolean[rawList.size()];

            for (int j = 0; j < rawList.size(); j++) {
                Object item = rawList.get(j);
                if (item == null) {
                    rowFlag[j] = false;
                    rowElements.add(VariableElement.newElement(new byte[0]));
                } else {
                    rowFlag[j] = true;
                    rowElements.add(VariableElement.newStringElement(item.toString()));
                }
            }
            lists.add(rowElements);
            perFlags.add(rowFlag);
        }
        return new Column(name, session.getSessionData().getMemoryGroup().getName(), lists, perFlags, colFlags, 0, rowCount, ColumnMetaData.ColumnDType.LIST_STRING);
    }

    public QueryExecution getExecution() {
        return execution;
    }

    public boolean isCancelled() {
        return isCancelled;
    }
}