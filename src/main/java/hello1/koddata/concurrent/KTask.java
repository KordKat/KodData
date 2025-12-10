package hello1.koddata.concurrent;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.ColumnArray;
import hello1.koddata.dataframe.ColumnMetaData;
import hello1.koddata.engine.*;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.Session;
import hello1.koddata.utils.collection.ImmutableArray;

import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class KTask implements Supplier<Value<?>> {

    private final QueryExecution execution;
    private final Session session;
    private final ColumnArray columnArray;
    private volatile boolean isCancelled = false; // เพิ่ม volatile เพื่อความปลอดภัยใน Thread

    public KTask(QueryExecution execution, ColumnArray columnArray, Session session) {
        this.execution = execution;
        this.columnArray = columnArray;
        this.session = session;
    }

    @Override
    public Value<?> get() {
        // 1. หาจำนวนแถว (Rows) ทั้งหมด
        int rows = columnArray.getColumns()
                .values()
                .stream()
                .findFirst() // ใช้ findFirst เพื่อความเร็ว
                .map(Column::size)
                .orElse(0);

        // 2. เตรียมโครงสร้างข้อมูล
        List<Value<?>> bufferValue = new ArrayList<>();
        // เก็บ ColumnOperation ที่รอการประมวลผลเมื่อครบทุกแถว
        QueryOperationNode pendingColumnOpNode = null;
        Map<String, Value<?>> columnOperationResultMap = new HashMap<>();

        // เตรียมถังเก็บผลลัพธ์ (Final Result)
        Set<String> columnNames = columnArray.getColumns().keySet();
        Map<String, List<Value<?>>> finalResult = new LinkedHashMap<>();
        for (String colName : columnNames) {
            finalResult.put(colName, new ArrayList<>(rows));
        }

        int index = 0;

        // 3. เริ่มวนลูปทีละแถว
        while (index < rows) {
            // -- Context Construction --
            // สร้าง Map ของค่าในแถวปัจจุบัน (Current Row Context)
            final int currentIndex = index;
            Map<String, Value<?>> valueMap = new HashMap<>();

            for (String colName : columnNames) {
                valueMap.put(colName, columnArray.getColumns().get(colName).readRow(currentIndex));
            }

            // ใส่ผลลัพธ์จาก ColumnOperation ก่อนหน้า (ถ้ามี)
            if (!columnOperationResultMap.isEmpty()) {
                valueMap.putAll(columnOperationResultMap);
            }

            // -- Pipeline Execution --
            // เริ่มต้นที่ Node แรก ถ้ามี pending op ให้ข้ามไป process logic การสะสมค่าแทน
            QueryOperationNode currentNode = (pendingColumnOpNode == null)
                    ? execution.getHead().getNextNode()
                    : pendingColumnOpNode;

            boolean rowProcessed = false;

            while (currentNode != null) {
                QueryOperation op = currentNode.getOperation();

                if (op instanceof ColumnOperation) {
                    // กรณีเจอ Column Operation (เช่น SUM, AVG)
                    // เราต้อง "หยุด" การไหลของข้อมูลปกติ และเริ่ม "สะสม" (Buffer)

                    // หมายเหตุ: โค้ดนี้สมมติว่า Node ก่อนหน้าได้ส่งค่ามาแล้ว หรือเราต้องดึงจาก valueMap
                    // ในบริบทนี้ ถ้า flow มาถึงนี่ แสดงว่า valueMap มีค่าล่าสุดที่ process มาแล้ว
                    // แต่เนื่องจาก logic เดิมซับซ้อน ส่วนนี้อาจจะต้องปรับตาม Data Flow ของคุณ
                    // สมมติว่า ColumnOperation รับค่าจาก Buffer ที่สะสมมาจาก Node ก่อนหน้า

                    pendingColumnOpNode = currentNode; // set flag ว่าเราติดสถานะ buffering

                    // *Logic นี้อาจต้องปรับตามว่า Input ของ ColumnOp มาจากไหน*
                    // สมมติว่ารับจาก Result ล่าสุดที่คำนวณได้
                    // ตรงนี้ Value ล่าสุดอาจจะต้องถูก tracking ไว้ แต่ใน code เดิมไม่ได้ส่งต่อชัดเจน
                    // ผมจะสมมติว่า ColumnOp รอ process ตอนจบ loop ใหญ่
                    break;
                }

                // กรณี Standard Operation (Row-by-Row)
                try {
                    String reqColumn = currentNode.getColumn();
                    Value<?> value = valueMap.get(reqColumn);

                    // ถ้าไม่มีค่าใน Map ให้ลองหาจาก Result Map หรือ Throw Error
                    if (value == null) {
                        if (columnOperationResultMap.containsKey(reqColumn)) {
                            value = columnOperationResultMap.get(reqColumn);
                        } else {
                            throw new KException(
                                    ExceptionCode.KD00005,
                                    "Dataframe does not have column: " + reqColumn
                            );
                        }
                    }

                    // Execute Operation
                    Value<?> result = op.operate(value);
                    if (result == null) break;

                    // Update valueMap เพื่อให้ Node ถัดไปใช้ค่านี้ได้
                    valueMap.put(reqColumn, result);

                    // เช็ค Node ถัดไป
                    QueryOperationNode nextNode = currentNode.getNextNode();

                    // ถ้า Node ถัดไปเป็น ColumnOperation -> เริ่ม Buffer
                    if (nextNode != null && nextNode.getOperation() instanceof ColumnOperation) {
                        bufferValue.add(result);
                        pendingColumnOpNode = nextNode; // Set Barrier

                        // ถ้าเป็นแถวสุดท้าย ให้ Execute Column Operation เลย
                        if (index == rows - 1) {
                            ColumnOperation co = (ColumnOperation) nextNode.getOperation();
                            Value<?> colResult = co.operate(new Value<>(new ArrayList<>(bufferValue)));

                            columnOperationResultMap.put(nextNode.getColumn(), colResult);
                            bufferValue.clear();
                            // pendingColumnOpNode = null; // Reset หรือไปต่อตาม Flow
                            // ในที่นี้ถ้าจบแล้วก็จบเลย หรือถ้ามี Node ต่อจาก ColumnOp ก็ต้อง handle ต่อ
                        }
                        break; // จบการ process แถวนี้ ไปแถวถัดไปเพื่อสะสมค่าต่อ
                    }

                    currentNode = nextNode;

                } catch (KException e) {
                    throw new RuntimeException("Error processing row " + index, e);
                }
            }

            // -- Result Collection --
            // บันทึกผลลัพธ์ของแถวนี้ลง Final Result
            for (String s : columnNames) {
                finalResult.get(s).add(valueMap.get(s));
            }

            index++;
        }

        // 4. สร้าง Columns ใหม่จากผลลัพธ์ (Reconstruct Dataframe)
        Map<String, Column> newColumns = new LinkedHashMap<>();

        for (Map.Entry<String, List<Value<?>>> entry : finalResult.entrySet()) {
            String colName = entry.getKey();
            List<Value<?>> values = entry.getValue();
            ColumnMetaData.ColumnDType type = inferType(values);

            try {
                Column newCol = new Column(colName, values, type);
                newColumns.put(colName, newCol);
            } catch (Exception e) {
                throw new RuntimeException("Error building column " + colName, e);
            }
        }

        return new Value<>(new ColumnArray(
                new ImmutableArray<>(new ArrayList<>(newColumns.values()))
        ));
    }

    // ปรับปรุง Infer Type ให้อ่านง่ายและครอบคลุม
    private ColumnMetaData.ColumnDType inferType(List<Value<?>> values) {
        Object sample = values.stream()
                .map(Value::get)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);

        if (sample == null) return ColumnMetaData.ColumnDType.SCALAR_STRING;

        // Pattern matching หรือ logic แบบง่าย
        if (sample instanceof List) {
            return inferListType((List<?>) sample);
        }

        return inferScalarType(sample);
    }

    private ColumnMetaData.ColumnDType inferScalarType(Object sample) {
        if (sample instanceof Integer || sample instanceof Long || sample instanceof Short || sample instanceof Byte)
            return ColumnMetaData.ColumnDType.SCALAR_INT;
        if (sample instanceof Double || sample instanceof Float)
            return ColumnMetaData.ColumnDType.SCALAR_DOUBLE;
        if (sample instanceof Boolean)
            return ColumnMetaData.ColumnDType.SCALAR_LOGICAL;
        if (sample instanceof LocalDate)
            return ColumnMetaData.ColumnDType.SCALAR_DATE;
        if (sample instanceof Instant)
            return ColumnMetaData.ColumnDType.SCALAR_TIMESTAMP;
        return ColumnMetaData.ColumnDType.SCALAR_STRING;
    }

    private ColumnMetaData.ColumnDType inferListType(List<?> list) {
        if (list.isEmpty()) return ColumnMetaData.ColumnDType.LIST_STRING;

        Object inner = list.stream().filter(Objects::nonNull).findFirst().orElse(null);
        if (inner == null) return ColumnMetaData.ColumnDType.LIST_STRING;

        if (inner instanceof Integer || inner instanceof Long || inner instanceof Short || inner instanceof Byte)
            return ColumnMetaData.ColumnDType.LIST_INT;
        if (inner instanceof Double || inner instanceof Float)
            return ColumnMetaData.ColumnDType.LIST_DOUBLE;
        if (inner instanceof Boolean)
            return ColumnMetaData.ColumnDType.LIST_LOGICAL;
        if (inner instanceof LocalDate)
            return ColumnMetaData.ColumnDType.LIST_DATE;
        if (inner instanceof Instant)
            return ColumnMetaData.ColumnDType.LIST_TIMESTAMP;

        return ColumnMetaData.ColumnDType.LIST_STRING;
    }

    public QueryExecution getExecution() {
        return execution;
    }

    public boolean isCancelled() {
        return isCancelled;
    }
}