package hello1.koddata.engine;

public class XorOperation implements QueryOperation {

    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null)
            return value;

        if (!(value.get() instanceof Boolean b))
            return value;

        // ค่า XOR true = toggle (กลับค่า)
        return new Value<>(!b);
    }
}