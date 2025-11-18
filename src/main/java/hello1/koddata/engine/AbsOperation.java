package hello1.koddata.engine;

public class AbsOperation implements QueryOperation {
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null) {
            return null;
        }
//        Number number = (Number) value.get();

        if(!(value.get() instanceof Number number)){
            return value;
        }

        double result = Math.abs(number.doubleValue());

        return new Value<>(result);
    }
}