package hello1.koddata.engine;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class MapOperation implements QueryOperation {

    private final Function<Object, Object> mapper;

    public MapOperation(Function<Object, Object> mapper) {
        this.mapper = mapper;
    }

    @Override
    public Value<?> operate(Value<?> value) {

        if (value == null || value.get() == null)
            return value;

        Object v = value.get();

        if (v instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            for (Object item : list) {
                out.add(mapper.apply(item)); // แปลงทีละตัว
            }
            return new Value<>(out);
        }

        if (v instanceof Object[] arr) {
            Object[] out = new Object[arr.length];
            for (int i = 0; i < arr.length; i++) {
                out[i] = mapper.apply(arr[i]);
            }
            return new Value<>(out);
        }

        return value;
    }
}
