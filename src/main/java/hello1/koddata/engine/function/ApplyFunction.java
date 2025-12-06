package hello1.koddata.engine.function;

import hello1.koddata.dataframe.ColumnArray;
import hello1.koddata.engine.QueryExecution;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.Session;

// เอา CompletableFuture import ออก เพราะเราจะรอผลลัพธ์เลย
// import java.util.concurrent.CompletableFuture;

// เปลี่ยน Generic Type จาก CompletableFuture<Value<?>> เป็น Object (หรือประเภทผลลัพธ์ที่แท้จริง)
public class ApplyFunction extends KodFunction<Object> {

    @Override
    public Value<Object> execute() throws KException {
        if(arguments.get("session") == null){
            throw new KException(ExceptionCode.KDE0012, "session not found");
        }
        Value<?> sessionV = arguments.get("session");
        if(!(sessionV.get() instanceof Session session)){
            throw new KException(ExceptionCode.KD00005, "not a session");
        }

        if(arguments.get("dataframe") == null){
            throw new KException(ExceptionCode.KDE0012, "dataframe not found");
        }

        Value<?> dataframeV = arguments.get("dataframe");
        if(!(dataframeV.get() instanceof ColumnArray columnArray)){
            throw new KException(ExceptionCode.KD00005, "not a dataframe");
        }

        if(arguments.get("operation") == null){
            throw new KException(ExceptionCode.KDE0012, "not found operation");
        }
        Value<?> operationV = arguments.get("operation");
        if(!(operationV.get() instanceof QueryExecution operation)){
            throw new KException(ExceptionCode.KDE0012, "not an operation");
        }

        try {
            // เรียก process และใช้ .join() เพื่อรอผลลัพธ์ทันที
            // สมมติว่า session.newProcess คืนค่า CompletableFuture<Value<?>>
            Value<?> result = session.newProcess(operation, columnArray);

            // Cast ผลลัพธ์กลับเป็น Value<Object> เพื่อให้ตรงกับ Signature ของ Method
            return (Value<Object>) result;

        } catch (Exception e) {
            // จัดการ Error กรณีที่ Process ทำงานล้มเหลว
            // อาจจะต้อง unwrap CompletionException หากจำเป็น
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new KException(ExceptionCode.KD00005, "Process execution failed: " + cause.getMessage());
        }
    }
}