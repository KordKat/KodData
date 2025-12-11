package hello1.koddata.engine;

//Inheritance
public class RadOperation implements QueryOperation {

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null)
            return value;
        if(!(value.get() instanceof Number number)){
            return value;
        }
        return new Value<>(Math.toRadians(number.doubleValue()));
    }
}