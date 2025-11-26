package hello1.koddata.engine;
import java.util.List;
import java.util.function.BiFunction;

public class ReduceOperation implements QueryOperation {

    private final Object identity;
    private final BiFunction<Object, Object, Object> reducer;

    public ReduceOperation(Object identity,
                           BiFunction<Object, Object, Object> reducer) {
        this.identity = identity;
        this.reducer = reducer;
    }

    @Override
    public Value<?> operate(Value<?> value) {

        if (value == null || value.get() == null)
            return value;

        Object data = value.get();

        Object result = identity;

        if (data instanceof List<?> list) {
            for (Object item : list) {
                result = reducer.apply(result, item); // รวมค่าเรื่อย ๆ
            }
            return new Value<>(result);
        }

        if (data instanceof Object[] arr) {
            for (Object item : arr) {
                result = reducer.apply(result, item);
            }
            return new Value<>(result);
        }

        return value;
    }
}
