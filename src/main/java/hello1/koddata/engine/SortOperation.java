package hello1.koddata.engine;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.util.*;
// array.sort

public class SortOperation implements ColumnOperation {

    @Override
    public Value<?> operate(Value<?> value) throws KException {

        if (!(value.get() instanceof List<?> column)) {
            throw new KException(ExceptionCode.KD00005, "Only list is accept");
        }

        int rows = column.size();

        Integer[] index = new Integer[rows];
        for (int i = 0; i < rows; i++) index[i] = i;

        Arrays.sort(index, (a, b) -> {
            try {
                Object oa = ((Value<?>) column.get(a)).get();
                Object ob = ((Value<?>) column.get(b)).get();

                if (oa == null && ob == null) return 0;
                if (oa == null) return 1;
                if (ob == null) return -1;

                if (oa instanceof Comparable ca && ob instanceof Comparable cb) {
                    return ca.compareTo(cb);
                }

                return 0;

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return new Value<>(Arrays.asList(index));
    }
}