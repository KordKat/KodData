package hello1.koddata.engine;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.util.List;

public class RangeOperation implements ColumnOperation {

    private final MaxOperation maxOp = new MaxOperation();
    private final MinOperation minOp = new MinOperation();

    @Override
    public Value<?> operate(Value<?> value) throws KException {

        if (!(value.get() instanceof List<?>)) {
            throw new KException(ExceptionCode.KD00005, "Only list is accept");
        }

        // ได้ max และ min จาก operation ที่เป็น list-based แล้ว
        Value<?> maxVal = maxOp.operate(value);
        Value<?> minVal = minOp.operate(value);

        Object maxRaw = maxVal.get();
        Object minRaw = minVal.get();

        // คำนวณ range ได้เฉพาะเมื่อทั้งคู่เป็น Number
        if (maxRaw instanceof Number maxNum && minRaw instanceof Number minNum) {
            double range = maxNum.doubleValue() - minNum.doubleValue();
            return new Value<>(range);
        }

        return new Value<>(Double.NaN);
    }
}
