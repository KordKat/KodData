package hello1.koddata.engine;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.DataFrameCursor;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.util.*;

public class SortOperation implements ColumnOperation {
// array.sort
    @Override
    public Value<?> operate(Value<?> value) throws KException {

        if (!(value.get() instanceof Column column)) {
            throw new KException(ExceptionCode.KD00005, "Only column is accept");
        }

        int rows = Math.toIntExact(column.getMetaData().getRows());

        Integer[] index = new Integer[rows];
        for (int i = 0; i < rows; i++) index[i] = i;

        DataFrameCursor c1 = new DataFrameCursor();
        DataFrameCursor c2 = new DataFrameCursor();

        Arrays.sort(index, (a, b) -> {
            try {
                Object oa = column.readRow(a, c1).get();
                Object ob = column.readRow(b, c2).get();

                if (oa == null && ob == null) return 0;
                if (oa == null) return 1;
                if (ob == null) return -1;

                if (oa instanceof Comparable ca && ob instanceof Comparable cb)
                    return ca.compareTo(cb);
                return 0;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return new Value<>(Arrays.asList(index));
    }
}
