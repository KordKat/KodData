package hello1.koddata.engine;

public class ConcatOperation implements QueryOperation {
    private final String suffix;

    public ConcatOperation(String suffix){
        this.suffix = suffix;
    }

    @Override
    public Value<?> operate(Value<?> value){
        if (value == null || value.get() == null) return value;
        return new Value<>(value.get().toString() + suffix);
    }
}
