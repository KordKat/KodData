package hello1.koddata.engine;

public class NotOperation implements QueryOperation {
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null) {
            return new Value<>(true);
        }
        return new Value<>(!Boolean.TRUE.equals(value.get()));
    }
}
