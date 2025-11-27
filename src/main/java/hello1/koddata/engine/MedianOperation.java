package hello1.koddata.engine;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.DataFrameCursor;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MedianOperation implements ColumnOperation {

    @Override
    public Value<?> operate(Value<?> value) throws KException {
        if (!(value.get() instanceof Column column)) {
            throw new KException(ExceptionCode.KD00005, "Only column is accept");
        }

        long rows = column.getMetaData().getRows();
        DataFrameCursor cursor = new DataFrameCursor();

        List<Double> list = new ArrayList<>();

        for (long i = 0; i < rows; i++) {
            Value<?> cell = column.readRow(Math.toIntExact(i), cursor);
            if (cell instanceof NullValue) continue;

            Object raw = cell.get();
            if (raw instanceof Number n) {
                list.add(n.doubleValue());
            }
        }

        int n = list.size();
        if (n == 0) {
            return new Value<>(Double.NaN);
        }

        Collections.sort(list);

        double median;
        if (n % 2 == 1) {
            median = list.get(n / 2);
        } else {
            double a = list.get((n / 2) - 1);
            double b = list.get(n / 2);
            median = (a + b) / 2.0;
        }

        return new Value<>(median);
    }
}
