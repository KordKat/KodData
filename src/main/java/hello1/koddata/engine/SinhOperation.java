package hello1.koddata.engine;

//Inheritance
public class SinhOperation implements QueryOperation {

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null)
            return value;

        if (!(value.get() instanceof Number n))
            return value;

        return new Value<>(Math.sinh(n.doubleValue()));
    }
}
