package hello1.koddata.concurrent;

import hello1.koddata.dataframe.ColumnArray;
import hello1.koddata.dataframe.DataFrameCursor;
import hello1.koddata.engine.*;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KTask extends Thread {

    private final QueryExecution execution;
    private boolean isCancelled = false;
    private ColumnArray columnArray;
    private String asName;
    public KTask(QueryExecution execution, ColumnArray columnArray) {
        this.execution = execution;
        this.columnArray = columnArray;
    }

    @Override
    public void run() {

        Map<String, DataFrameCursor> cursorMap = columnArray.getColumns().keySet().stream().collect(Collectors.toMap(Function.identity(), k -> new DataFrameCursor()));

        int index = 0;
        int rows = columnArray.getColumns().values().stream().findAny().get().getMetaData().getRows();
        List<Value<?>> bufferValue = new ArrayList<>();
        Map<Integer, QueryOperationNode> barrier = new HashMap<>();
        Map<String, Value<?>> columnOperationResultMap = new HashMap<>();

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
            index++;
        }
    }

    public QueryExecution getExecution() {
        return execution;
    }

    public void cancel() {
        this.interrupt();
        isCancelled = true;
    }

    public boolean isCancelled() {
        return isCancelled;
    }
}