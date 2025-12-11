package hello1.koddata.engine;

//Inheritance
public class ACosOperation implements QueryOperation{

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null) return value;
        if (!(value.get() instanceof Number n)) return value;

        return new Value<>(Math.acos(n.doubleValue()));
    }
}
