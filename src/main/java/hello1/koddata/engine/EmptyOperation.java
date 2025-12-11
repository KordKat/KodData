package hello1.koddata.engine;

//Inheritance
public class EmptyOperation implements QueryOperation {

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        return value;
    }
}