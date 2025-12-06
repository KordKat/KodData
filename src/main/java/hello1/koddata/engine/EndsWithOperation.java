package hello1.koddata.engine;

public class EndsWithOperation implements QueryOperation {

    private final String suffix;

    public EndsWithOperation(String suffix){
        this.suffix = suffix;
    }

    @Override
    public Value<?> operate(Value<?> value){
        if (value == null || value.get() == null) return value;

        if (value.get() instanceof String s)
            return new Value<>(s.endsWith(suffix));

        return value;
    }
}
