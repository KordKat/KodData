package hello1.koddata.engine.function;

import hello1.koddata.dataframe.ColumnArray;
import hello1.koddata.engine.QueryExecution;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.Session;

import java.util.concurrent.CompletableFuture;

//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ
public class ApplyFunction extends KodFunction<CompletableFuture<ColumnArray>> {

//   Polymorphism
    @Override
    public Value<CompletableFuture<ColumnArray>> execute() throws KException {
        if (arguments.get("session") == null) {
            throw new KException(ExceptionCode.KDE0012, "session not found");
        }
        Value<?> sessionV = arguments.get("session");
        if (!(sessionV.get() instanceof Session session)) {
            throw new KException(ExceptionCode.KD00005, "not a session");
        }

        if (arguments.get("dataframe") == null) {
            throw new KException(ExceptionCode.KDE0012, "dataframe not found");
        }

        Value<?> dataframeV = arguments.get("dataframe");
        if (!(dataframeV.get() instanceof ColumnArray columnArray)) {
            throw new KException(ExceptionCode.KD00005, "not a dataframe");
        }

        if (arguments.get("operation") == null) {
            throw new KException(ExceptionCode.KDE0012, "not found operation");
        }
        Value<?> operationV = arguments.get("operation");
        if (!(operationV.get() instanceof QueryExecution operation)) {
            throw new KException(ExceptionCode.KDE0012, "not an operation");
        }

        try {
            CompletableFuture<Value<?>> result = session.newProcess(operation, columnArray);

            CompletableFuture<ColumnArray> finalFuture = result.thenApply(value -> {
                Object raw = value.get();
                if (raw instanceof ColumnArray ca) {
                    return ca;
                }
                throw new IllegalStateException("Process result was expected to be a ColumnArray but was: " + raw.getClass().getName());
            });

            return new Value<>(finalFuture);

        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new KException(ExceptionCode.KD00005, "Process execution failed: " + cause.getMessage());
        }
    }
}