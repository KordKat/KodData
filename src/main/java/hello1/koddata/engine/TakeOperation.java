package hello1.koddata.engine;

import hello1.koddata.dataframe.Column;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.util.ArrayList;
import java.util.List;

public class TakeOperation implements ColumnOperation {

    private final int limit;

    public TakeOperation(int limit) {
        this.limit = Math.max(0, limit);
    }

    @Override
    public Value<?> operate(Value<?> value) throws KException {

        if (!(value.get() instanceof Column column))
            throw new KException(ExceptionCode.KD00005, "Only column is accept");

        int rows = Math.toIntExact(column.getMetaData().getRows());

        List<Integer> result = new ArrayList<>();

        for (int i = 0; i < rows && i < limit; i++) {
            result.add(i);
        }

        return new Value<>(result);
    }
}
