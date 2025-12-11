package hello1.koddata.engine;

//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ
public class IsBoolOperation implements QueryOperation{

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        if(value.get() instanceof Boolean){
            return new Value<>(Boolean.TRUE);
        }
        return new Value<>(Boolean.FALSE);
    }
}
