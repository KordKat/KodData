package hello1.koddata.engine;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.util.List;

public class MinOperation implements ColumnOperation {

    @Override
    public Value<?> operate(Value<?> value) throws KException {

        if (!(value.get() instanceof List<?> column)) {
            throw new KException(ExceptionCode.KD00005, "Only list is accept");
        }

        double min = Double.POSITIVE_INFINITY;
        boolean hasValue = false;

        for (Object o : column) {

            if (!(o instanceof Value<?> cell)) {
                continue;
            }

            if (cell instanceof NullValue) {
                continue;
            }

            Object raw = cell.get();

            if (raw instanceof Number n) {
                double d = n.doubleValue();
                if (!hasValue || d < min) {
                    min = d;
                    hasValue = true;
                }
            }
        }

        if (!hasValue) {
            return new Value<>(Double.NaN);
        }

        return new Value<>(min);
    }
}
