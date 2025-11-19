package hello1.koddata.engine;

public class UpperOperation implements QueryOperation{
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null)
            return value;
        if (!(value.get() instanceof String string)) {
            return value;
        }
        return new Value<>(string.toUpperCase());
    }
}
