package hello1.koddata.engine;

//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ
public class AbsOperation implements QueryOperation {

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null) {
            return value;
        }
//        Number number = (Number) value.get();

        if(!(value.get() instanceof Number number)){
            return value;
        }

        double result = Math.abs(number.doubleValue());

        return new Value<>(result);
    }
}