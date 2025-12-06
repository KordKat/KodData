package hello1.koddata.engine;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import java.util.List;

public class ProductOperation implements ColumnOperation {

    @Override
    public Value<?> operate(Value<?> value) throws KException {

        if (!(value.get() instanceof List<?> column)) {
            throw new KException(ExceptionCode.KD00005, "Only list is accept");
        }

        double product = 1.0;
        boolean hasValue = false;

        // column = List<Value<?>>
        for (Object o : column) {

            // ต้องเป็น Value<?> เท่านั้น
            if (!(o instanceof Value<?> cell)) {
                continue;
            }

            // NullValue ข้าม
            if (cell instanceof NullValue) {
                continue;
            }

            Object raw = cell.get();

            if (raw instanceof Number n) {
                product *= n.doubleValue();
                hasValue = true;
            }
        }

        // ถ้าไม่เจอเลขเลย NaN
        if (!hasValue) {
            return new Value<>(Double.NaN);
        }

        return new Value<>(product);
    }
}
