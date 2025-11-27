package hello1.koddata.engine;

import hello1.koddata.dataframe.Column;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.util.ArrayList;
import java.util.List;

public class SkipOperation implements ColumnOperation {

    private final int count;

    public SkipOperation(int count) {
        this.count = Math.max(0, count);
    }

    @Override
    public Value<?> operate(Value<?> value) throws KException {

        if (!(value.get() instanceof Column column))
            throw new KException(ExceptionCode.KD00005, "Only column is accept");

        int rows = Math.toIntExact(column.getMetaData().getRows());

        List<Integer> result = new ArrayList<>();

        for (int i = count; i < rows; i++) {
            result.add(i);
        }

        return new Value<>(result);
    }
}
