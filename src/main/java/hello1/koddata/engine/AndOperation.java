package hello1.koddata.engine;

public class AndOperation implements QueryOperation {

    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null)
            return value;

        if (!(value.get() instanceof Boolean b))
            return value;

        // ค่าใน pipeline จะเป็น Boolean อยู่แล้ว
        return new Value<>(b);
    }
}
