package hello1.koddata.engine;

import java.util.List;

//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ
public class IsListOperation implements QueryOperation{

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        if(value.get() instanceof List){
            return new Value<>(Boolean.TRUE);
        }
        return new Value<>(Boolean.FALSE);
    }
}
