package hello1.koddata.engine;

//Inheritance
public class IsNumberOperation implements QueryOperation{

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        if(value.get() instanceof Number){
            return new Value<>(Boolean.TRUE);
        }
        return new Value<>(Boolean.FALSE);
    }
}
