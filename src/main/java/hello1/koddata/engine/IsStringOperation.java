package hello1.koddata.engine;

//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ
public class IsStringOperation implements QueryOperation{

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        if(value.get() instanceof String){
            return new Value<>(Boolean.TRUE);
        }
        return new Value<>(Boolean.FALSE);
    }
}
