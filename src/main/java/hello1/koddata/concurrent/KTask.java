package hello1.koddata.concurrent;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.ColumnArray;
import hello1.koddata.dataframe.ColumnMetaData;
import hello1.koddata.engine.*;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.Session;
import hello1.koddata.utils.collection.ImmutableArray;

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
    private final Session session;
    private final ColumnArray columnArray;

    public KTask(QueryExecution execution, ColumnArray columnArray, Session session) {
        this.execution = execution;
        this.columnArray = columnArray;
        this.session = session;
    }

    @Override
    public Value<?> get() {

        int rows = columnArray.getColumns()
                .values()
                .stream()
                .findAny()
                .map(Column::size)
                .orElse(0);

        int index = 0;

        List<Value<?>> bufferValue = new ArrayList<>();
        Map<Integer, QueryOperationNode> barrier = new HashMap<>();
        Map<String, Value<?>> columnOperationResultMap = new HashMap<>();
        Map<String, List<Value<?>>> finalResult = columnArray.getColumns()
                .keySet()
                .stream()
                .collect(Collectors.toMap(Function.identity(), x -> new ArrayList<>()));

        while (index < rows) {
            if (barrier.containsKey(index)) {

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

            Map<String, Value<?>> valueMap = columnArray.getColumns()
                    .keySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Function.identity(),
                            k -> columnArray.getColumns().get(k).readRow(finalIndex)
                    ));
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

                        if (value == null) {
                            if (columnOperationResultMap.containsKey(reqColumn)) {
                                value = columnOperationResultMap.get(reqColumn);
                            } else {
                                throw new KException(
                                        ExceptionCode.KD00005,
                                        "Dataframe does not have column, " + reqColumn
                                );
                            }
                        }
                        Value<?> result = op.operate(value);
                        if (result == null) break;
                        if (next.getNextNode() != null &&
                                next.getNextNode().getOperation() instanceof ColumnOperation) {

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
            for (String s : columnArray.getColumns().keySet()) {
                finalResult.get(s).add(valueMap.get(s));
            }

            index++;
        }

        Map<String, Column> newColumns = new HashMap<>();
        finalResult.values().stream().findFirst().map(List::size);

        for (String colName : finalResult.keySet()) {
            List<Value<?>> values = finalResult.get(colName);

            ColumnMetaData.ColumnDType type = inferType(values);

            try {
                Column newCol = new Column(colName, values, type);
                newColumns.put(colName, newCol);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Error building column " + colName + " with inferred type " + type, e
                );
            }
        }
        return new Value<>(new ColumnArray(
                new ImmutableArray<>(new ArrayList<>(newColumns.values()))
        ));
    }
    private ColumnMetaData.ColumnDType inferType(List<Value<?>> values) {
        Object sample = values.stream()
                .map(Value::get)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);

        if (sample == null) {
            return ColumnMetaData.ColumnDType.SCALAR_STRING;
        }

        if (sample instanceof Integer ||
                sample instanceof Long ||
                sample instanceof Short ||
                sample instanceof Byte) {
            return ColumnMetaData.ColumnDType.SCALAR_INT;
        }

        if (sample instanceof Double || sample instanceof Float) {
            return ColumnMetaData.ColumnDType.SCALAR_DOUBLE;
        }

        if (sample instanceof Boolean) {
            return ColumnMetaData.ColumnDType.SCALAR_LOGICAL;
        }

        if (sample instanceof java.time.LocalDate) {
            return ColumnMetaData.ColumnDType.SCALAR_DATE;
        }

        if (sample instanceof java.time.Instant) {
            return ColumnMetaData.ColumnDType.SCALAR_TIMESTAMP;
        }

        if (sample instanceof String) {
            return ColumnMetaData.ColumnDType.SCALAR_STRING;
        }
        if (sample instanceof List) {
            List<?> list = (List<?>) sample;
            if (list.isEmpty()) {
                return ColumnMetaData.ColumnDType.LIST_STRING;
            }

            Object inner = list.stream().filter(Objects::nonNull).findFirst().orElse(null);
            if (inner == null) {
                return ColumnMetaData.ColumnDType.LIST_STRING;
            }

            if (inner instanceof Integer ||
                    inner instanceof Long ||
                    inner instanceof Short ||
                    inner instanceof Byte) {
                return ColumnMetaData.ColumnDType.LIST_INT;
            }

            if (inner instanceof Double || inner instanceof Float) {
                return ColumnMetaData.ColumnDType.LIST_DOUBLE;
            }

            if (inner instanceof Boolean) {
                return ColumnMetaData.ColumnDType.LIST_LOGICAL;
            }

            if (inner instanceof java.time.LocalDate) {
                return ColumnMetaData.ColumnDType.LIST_DATE;
            }

            if (inner instanceof java.time.Instant) {
                return ColumnMetaData.ColumnDType.LIST_TIMESTAMP;
            }

            if (inner instanceof String) {
                return ColumnMetaData.ColumnDType.LIST_STRING;
            }
            return ColumnMetaData.ColumnDType.LIST_STRING;
        }
        return ColumnMetaData.ColumnDType.SCALAR_STRING;
    }

    public QueryExecution getExecution() {
        return execution;
    }

    public boolean isCancelled() {
        return isCancelled;
    }
}