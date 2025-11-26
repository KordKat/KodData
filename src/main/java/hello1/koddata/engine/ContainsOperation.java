package hello1.koddata.engine;

public class ContainsOperation implements QueryOperation {

    private final String search;

    public ContainsOperation(String search) {
        this.search = search;
    }

    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null) return value;

        if (value.get() instanceof String s)
            return new Value<>(s.contains(search));

        return value;
    }
}

