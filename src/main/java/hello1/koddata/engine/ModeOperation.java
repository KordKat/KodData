package hello1.koddata.engine;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ModeOperation implements ColumnOperation {

    @Override
    public Value<?> operate(Value<?> value) throws KException {

        if (!(value.get() instanceof List<?> column)) {
            throw new KException(ExceptionCode.KD00005, "Only list is accept");
        }

        Map<Double, Integer> freq = new HashMap<>();

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
                freq.merge(d, 1, Integer::sum);
            }
        }
        if (freq.isEmpty()) {
            return new Value<>(Double.NaN);
        }

        double mode = 0.0;
        int bestCount = -1;
        for (Map.Entry<Double, Integer> e : freq.entrySet()) {
            double val = e.getKey();
            int count = e.getValue();
            if (count > bestCount || (count == bestCount && val < mode)) {
                bestCount = count;
                mode = val;
            }
        }

        return new Value<>(mode);
    }
}
