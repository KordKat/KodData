package hello1.koddata.engine;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.util.List;

//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ
public class MeanOperation implements ColumnOperation {

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) throws KException {

        if (!(value.get() instanceof List<?> column)) {
            throw new KException(ExceptionCode.KD00005, "Only list is accept");
        }

        double sum = 0.0;
        long count = 0;

        // column = List<Value<?>>
        for (Object o : column) {

            // ต้องเป็น Value<?> เท่านั้น ถ้าไม่ใช่ให้ข้าม
            if (!(o instanceof Value<?> cell)) {
                continue;
            }

            // ไม่คิด NullValue
            if (cell instanceof NullValue) {
                continue;
            }

            Object raw = cell.get();

            if (raw instanceof Number n) {
                sum += n.doubleValue();
                count++;
            }
        }

        if (count == 0) {
            return new Value<>(Double.NaN);
        }

        return new Value<>(sum / count);
    }
}
