package hello1.koddata.engine;

public class XorOperation implements QueryOperation {

    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null)
            return value;

        if (!(value.get() instanceof Boolean b))
            return value;

        return new Value<>(!b);
    }
}