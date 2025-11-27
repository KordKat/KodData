package hello1.koddata.engine;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import java.util.List;

public class CountOperation implements ColumnOperation {

    @Override
    public Value<?> operate(Value<?> value) throws KException {

        if (!(value.get() instanceof List<?> column)) {
            throw new KException(ExceptionCode.KD00005, "Only list is accept");
        }

        long count = 0;

        // column = List<Value<?>>
        for (Object o : column) {

            if (!(o instanceof Value<?> cell)) {
                // ถ้า element ไม่ใช่ Value ก็ข้ามหรือโยน error
                continue;
            }

            // นับเฉพาะที่ไม่ใช่ NullValue
            if (cell instanceof NullValue) continue;

            count++;
        }

        return new Value<>(count);
    }
}
