package hello1.koddata.engine;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.DataFrameCursor;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

public class MaxOperation implements ColumnOperation {

    @Override
    public Value<?> operate(Value<?> value) throws KException {
        if (!(value.get() instanceof Column column)) {
            throw new KException(ExceptionCode.KD00005, "Only column is accept");
        }

        long rows = column.getMetaData().getRows();
        DataFrameCursor cursor = new DataFrameCursor();

        double max = Double.NEGATIVE_INFINITY;
        boolean hasValue = false;

        for (long i = 0; i < rows; i++) {
            Value<?> cell = column.readRow(Math.toIntExact(i), cursor);
            if (cell instanceof NullValue) continue;

            Object raw = cell.get();
            if (raw instanceof Number n) {
                double d = n.doubleValue();
                if (!hasValue || d > max) {
                    max = d;
                    hasValue = true;
                }
            }
        }

        if (!hasValue) {
            // ไม่มีค่าเลขเลยใน column
            return new Value<>(Double.NaN);
        }

        return new Value<>(max);
    }
}