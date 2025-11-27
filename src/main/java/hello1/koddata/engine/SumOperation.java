package hello1.koddata.engine;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.DataFrameCursor;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

public class SumOperation implements ColumnOperation {

    @Override
    public Value<?> operate(Value<?> value) throws KException {
        double acc = 0;

        if (!(value.get() instanceof Column column)) {
            throw new KException(ExceptionCode.KD00005, "Only column is accept");
        }

        long rows = column.getMetaData().getRows();
        DataFrameCursor cursor = new DataFrameCursor();

        for (long i = 0; i < rows; i++) {
            Value<?> val = column.readRow(Math.toIntExact(i), cursor);
            if (val instanceof NullValue) continue;

            if (val.get() instanceof Number n) {
                acc += n.doubleValue();
            }
        }

        return new Value<>(acc);
    }
}
