package hello1.koddata.engine;

//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ
public class ExpOperation implements QueryOperation {

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null)
            return value;
        if (!(value.get() instanceof Number number)) {
            return value;
        }
        return new Value<>(Math.exp(number.doubleValue()));
    }
}