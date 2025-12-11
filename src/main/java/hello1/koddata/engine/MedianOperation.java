package hello1.koddata.engine;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MedianOperation implements ColumnOperation {

    @Override
    public Value<?> operate(Value<?> value) throws KException {

        if (!(value.get() instanceof List<?> column)) {
            throw new KException(ExceptionCode.KD00005, "Only list is accept");
        }

        List<Double> numbers = new ArrayList<>();

        for (Object o : column) {

            if (!(o instanceof Value<?> cell)) {
                continue;
            }

            if (cell instanceof NullValue) {
                continue;
            }

            Object raw = cell.get();

            if (raw instanceof Number n) {
                numbers.add(n.doubleValue());
            }
        }
        int n = numbers.size();
        if (n == 0) {
            return new Value<>(Double.NaN);
        }
        Collections.sort(numbers);
        double median;
        if (n % 2 == 1) {
            median = numbers.get(n / 2);
        } else {
            double left = numbers.get((n / 2) - 1);
            double right = numbers.get(n / 2);
            median = (left + right) / 2.0;
        }

        return new Value<>(median);
    }
}
