package hello1.koddata.engine;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.DataFrameCursor;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import java.util.HashMap;
import java.util.Map;

public class ModeOperation implements ColumnOperation {

    @Override
    public Value<?> operate(Value<?> value) throws KException {
        if (!(value.get() instanceof Column column)) {
            throw new KException(ExceptionCode.KD00005, "Only column is accept");
        }

        long rows = column.getMetaData().getRows();
        DataFrameCursor cursor = new DataFrameCursor();

        Map<Double, Integer> freq = new HashMap<>();

        for (long i = 0; i < rows; i++) {
            Value<?> cell = column.readRow(Math.toIntExact(i), cursor);
            if (cell instanceof NullValue) continue;

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
