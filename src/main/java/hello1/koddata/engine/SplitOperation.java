package hello1.koddata.engine;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

//Inheritance
public class SplitOperation implements QueryOperation {
    // ทำ คล้าย filter แต่เพิ่ม node ใน QueryOperationNode
    private Predicate<Value<?>> predicate;

    public SplitOperation(Predicate<Value<?>> predicate){
    this.predicate = predicate;}

    public SplitOperation(){
        this(value -> true);
    }

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        return predicate.test(value) ? value : null;
    }


}

