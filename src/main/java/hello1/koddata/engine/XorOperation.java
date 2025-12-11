package hello1.koddata.engine;

//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ
public class XorOperation implements QueryOperation {

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null)
            return value;

        if (!(value.get() instanceof Boolean b))
            return value;

        return new Value<>(!b);
    }
}