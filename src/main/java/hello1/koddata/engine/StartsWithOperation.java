package hello1.koddata.engine;

public class StartsWithOperation implements QueryOperation {

    private final String prefix;

    public StartsWithOperation(String prefix){
        this.prefix = prefix;
    }

    @Override
    public Value<?> operate(Value<?> value){
        if (value == null || value.get() == null) return value;

        if (value.get() instanceof String s)
            return new Value<>(s.startsWith(prefix));

        return value;
    }
}
