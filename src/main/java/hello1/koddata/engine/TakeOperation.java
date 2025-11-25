package hello1.koddata.engine;

import java.util.List;

public class TakeOperation implements QueryOperation {

    private final int limit;

    public TakeOperation(int limit) {
        this.limit = limit;
    }

    @Override
    public Value<?> operate(Value<?> value) {
        List<?> list = (List<?>) value.get();

        int size = list.size();
        Object[] result = new Object[size];

        for (int i = 0; i < size; i++) {
            if (i < limit) {
                result[i] = list.get(i);
            } else {
                result[i] = null;
            }
        }

        return new Value<>(result);
    }
}
