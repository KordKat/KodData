package hello1.koddata.engine;

//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ
public class FillOperation implements QueryOperation {

    private Value<?> value;
    private Value<?> fillValue;

    public FillOperation(Value<?> value, Value<?> fillValue){
        this.value = value;
        this.fillValue = fillValue;
    }

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        return value.get().equals(this.value.get()) ? fillValue : value;
    }
}