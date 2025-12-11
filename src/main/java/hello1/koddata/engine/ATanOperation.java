package hello1.koddata.engine;

//Inheritance
public class ATanOperation implements QueryOperation {

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null) return value;
        if (!(value.get() instanceof Number n)) return value;

        return new Value<>(Math.atan(n.doubleValue()));
    }
}
