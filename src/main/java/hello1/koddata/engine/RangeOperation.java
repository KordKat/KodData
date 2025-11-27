package hello1.koddata.engine;

import hello1.koddata.dataframe.Column;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

public class RangeOperation implements ColumnOperation {

    private final MaxOperation maxOp = new MaxOperation();
    private final MinOperation minOp = new MinOperation();

    @Override
    public Value<?> operate(Value<?> value) throws KException {
        if (!(value.get() instanceof Column)) {
            throw new KException(ExceptionCode.KD00005, "Only column is accept");
        }

        Value<?> maxVal = maxOp.operate(value);
        Value<?> minVal = minOp.operate(value);

        Object maxRaw = maxVal.get();
        Object minRaw = minVal.get();

        if (maxRaw instanceof Number maxNum && minRaw instanceof Number minNum) {
            double range = maxNum.doubleValue() - minNum.doubleValue();
            return new Value<>(range);
        }

        return new Value<>(Double.NaN);
    }
}
