package hello1.koddata.engine;

import java.util.Objects;

//Inheritance
public class EqualsOperation implements QueryOperation {
    private final Object target;

    public EqualsOperation(Object target) {
        this.target = target;
    }

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null) return new Value<>(target == null);
        return new Value<>(Objects.equals(value.get(), target));
    }
}
