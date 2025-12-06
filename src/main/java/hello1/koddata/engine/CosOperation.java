package hello1.koddata.engine;

public class CosOperation implements QueryOperation {
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null)
            return value;
        if (!(value.get() instanceof Number n))
            return value;

        return new Value<>(Math.cos(n.doubleValue()));
    }
}

