package hello1.koddata.engine;

public class MatchOperation implements QueryOperation {

    private final String regex;

    public MatchOperation(String regex){
        this.regex = regex;
    }

    @Override
    public Value<?> operate(Value<?> value){
        if (value == null || value.get() == null) return value;

        if (value.get() instanceof String s)
            return new Value<>(s.matches(regex));

        return value;
    }
}
