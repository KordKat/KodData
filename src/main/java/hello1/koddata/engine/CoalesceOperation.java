package hello1.koddata.engine;


//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ
public class CoalesceOperation implements QueryOperation{
    private Value<?> fillValue;

    public  CoalesceOperation(Value<?> fillValue){
        this.fillValue = fillValue;
    }

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {

        if(value.get() == null){
            return fillValue;
        }
        return value;
    }
}
