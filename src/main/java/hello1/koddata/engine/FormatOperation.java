package hello1.koddata.engine;

public class FormatOperation implements QueryOperation {

    private final String pattern;

    public FormatOperation(String pattern){
        this.pattern = pattern;
    }

    @Override
    public Value<?> operate(Value<?> value){
        if (value == null) return value;

        Object v = value.get();
        return new Value<>(String.format(pattern, v));
    }
}
