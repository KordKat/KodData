package hello1.koddata.engine;

//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ
public class NotOperation implements QueryOperation {

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null) {
            return new Value<>(true);
        }
        return new Value<>(!Boolean.TRUE.equals(value.get()));
    }
}
