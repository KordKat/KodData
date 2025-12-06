package hello1.koddata.engine;


public class CoshOperation implements QueryOperation {

    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null)
            return value;

        if (!(value.get() instanceof Number n))
            return value;

        return new Value<>(Math.cosh(n.doubleValue()));
    }
}
