package hello1.koddata.engine;

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

        if (!(value.get() instanceof List<?> column)) {
            throw new KException(ExceptionCode.KD00005, "Only list is accept");
        }

        List<Integer> result = new ArrayList<>();

        // take index ตามลำดับจาก list
        for (int i = 0; i < column.size() && i < limit; i++) {
            result.add(i);
        }

        return new Value<>(result);
    }
}
