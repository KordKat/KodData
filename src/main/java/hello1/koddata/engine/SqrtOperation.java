package hello1.koddata.engine;

//Inheritance
public class SqrtOperation implements QueryOperation {

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null) {
            return value;
        }
        if (!(value.get() instanceof Number number)) {
            return value;
        }
        double val = number.doubleValue();
        if (val < 0) {
            return value;
        }
        return new Value<>(Math.sqrt(val));
    }
}