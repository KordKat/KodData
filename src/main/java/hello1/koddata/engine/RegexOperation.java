package hello1.koddata.engine;

public class RegexOperation implements QueryOperation {

    private final String regex;
    private final String replacement;

    public RegexOperation(String regex, String replacement){
        this.regex = regex;
        this.replacement = replacement;
    }

    @Override
    public Value<?> operate(Value<?> value){
        if (value == null || value.get() == null) return value;

        if (value.get() instanceof String s)
            return new Value<>(s.replaceAll(regex, replacement));

        return value;
    }
}
