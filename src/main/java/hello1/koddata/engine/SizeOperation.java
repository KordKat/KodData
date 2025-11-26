package hello1.koddata.engine;

import java.util.List;

public class SizeOperation implements QueryOperation {
    @Override
    public Value<?> operate(Value<?> value){
        if (value == null || value.get() == null) return value;

        Object v = value.get();
        if (v instanceof List<?> list)

            return new Value<>(list.size());
        if (v instanceof Object[] arr)
            return new Value<>(arr.length);

        return value;
    }
}
