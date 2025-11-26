package hello1.koddata.engine;

public class SinhOperation implements QueryOperation {

    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null)
            return value;

        if (!(value.get() instanceof Number n))
            return value;

        return new Value<>(Math.sinh(n.doubleValue()));
    }
}
