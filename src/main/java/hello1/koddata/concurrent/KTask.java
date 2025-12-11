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

import java.util.function.Supplier;


// Inheritance
public class KTask implements Supplier<Value<?>> {
    // Encapsulation
    private final QueryExecution execution;
    private final Session session;
    private final ColumnArray columnArray;
    private volatile boolean isCancelled = false;

    public KTask(QueryExecution execution, ColumnArray columnArray, Session session) {
        this.execution = execution;
        this.columnArray = columnArray;
        this.session = session;
    }
    // Polymorphism
    @Override
    public Value<?> get() {
        int rows = columnArray.getColumns()
                .values()
                .stream()
                .findFirst()
                .map(Column::size)
                .orElse(0);

        List<Value<?>> bufferValue = new ArrayList<>();
        QueryOperationNode pendingColumnOpNode = null;
        Map<String, Value<?>> columnOperationResultMap = new HashMap<>();

        Set<String> columnNames = columnArray.getColumns().keySet();
        Map<String, List<Value<?>>> finalResult = new LinkedHashMap<>();
        for (String colName : columnNames) {
            finalResult.put(colName, new ArrayList<>(rows));
        }

        int index = 0;

        while (index < rows) {
            final int currentIndex = index;
            Map<String, Value<?>> valueMap = new HashMap<>();

            for (String colName : columnNames) {
                valueMap.put(colName, columnArray.getColumns().get(colName).readRow(currentIndex));
            }
            if (!columnOperationResultMap.isEmpty()) {
                valueMap.putAll(columnOperationResultMap);
            }
            QueryOperationNode currentNode = (pendingColumnOpNode == null)
                    ? execution.getHead().getNextNode()
                    : pendingColumnOpNode;

            boolean rowProcessed = false;

            while (currentNode != null) {
                QueryOperation op = currentNode.getOperation();
            // Abstract
                if (op instanceof ColumnOperation) {

                    pendingColumnOpNode = currentNode;

                    break;
                }

                try {
                    String reqColumn = currentNode.getColumn();
                    Value<?> value = valueMap.get(reqColumn);

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
                    // Polymorphism
                    Value<?> result = op.operate(value);
                    if (result == null) break;
                    valueMap.put(reqColumn, result);
                    QueryOperationNode nextNode = currentNode.getNextNode();
                    if (nextNode != null && nextNode.getOperation() instanceof ColumnOperation) {
                        bufferValue.add(result);
                        pendingColumnOpNode = nextNode;
                        if (index == rows - 1) {
                            ColumnOperation co = (ColumnOperation) nextNode.getOperation();
                            // Polymorphism + Abstract
                            Value<?> colResult = co.operate(new Value<>(new ArrayList<>(bufferValue)));

                            columnOperationResultMap.put(nextNode.getColumn(), colResult);
                            bufferValue.clear();
                        }
                        break;
                    }

                    currentNode = nextNode;

                } catch (KException e) {
                    throw new RuntimeException("Error processing row " + index, e);
                }
            }
            for (String s : columnNames) {
                finalResult.get(s).add(valueMap.get(s));
            }

            index++;
        }
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
    //Encapsulation
    private ColumnMetaData.ColumnDType inferType(List<Value<?>> values) {
        Object sample = values.stream()
                .map(Value::get)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);

        if (sample == null) return ColumnMetaData.ColumnDType.SCALAR_STRING;
        if (sample instanceof List) {
            return inferListType((List<?>) sample);
        }

        return inferScalarType(sample);
    }
    //Encapsulation
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
    // Encapsulation
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
    // Encapsulation
    public QueryExecution getExecution() {
        return execution;
    }

    public boolean isCancelled() {
        return isCancelled;
    }
}