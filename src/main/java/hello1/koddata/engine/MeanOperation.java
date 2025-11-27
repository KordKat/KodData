package hello1.koddata.engine;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.DataFrameCursor;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
public class MeanOperation implements ColumnOperation {

    @Override
    public Value<?> operate(Value<?> value) throws KException {
        if (!(value.get() instanceof Column column)) {
            throw new KException(ExceptionCode.KD00005, "Only column is accept");
        }

        long rows = column.getMetaData().getRows();
        DataFrameCursor cursor = new DataFrameCursor();

        double sum = 0.0;
        long count = 0;

        for (long i = 0; i < rows; i++) {
            Value<?> cell = column.readRow(Math.toIntExact(i), cursor);
            if (cell instanceof NullValue) continue;

            Object raw = cell.get();
            if (raw instanceof Number n) {
                sum += n.doubleValue();
                count++;
            }
        }

        if (count == 0) {
            return new Value<>(Double.NaN);
        }

        double avg = sum / count;
        return new Value<>(avg);
    }
}
