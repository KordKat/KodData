package hello1.koddata.engine;

import java.util.List;

public class JoinOperation implements QueryOperation {

    private final String delimiter;

    public JoinOperation(String delimiter){
        this.delimiter = delimiter;
    }

    @Override
    public Value<?> operate(Value<?> value){
        if (value == null || value.get() == null) return value;

        Object v = value.get();

        if (v instanceof List<?> list) {
            return new Value<>(String.join(delimiter, list.stream()
                    .map(Object::toString)
                    .toList()));
        }

        return value;
    }
}