package hello1.koddata.engine;

import hello1.koddata.exception.KException;

public class DropOperation implements QueryOperation {

    // เก็บชื่อ column ที่รับผ่าน constructor
    private final String columnName;

    public DropOperation(String columnName) {
        this.columnName = columnName; // รับผ่าน constructor ตรงนี้
    }

    @Override
    public Value<?> operate(Value<?> value) throws KException {
        // ส่งชื่อ column ที่ต้องการ drop กลับไป
        return new Value<>(columnName);
    }
}
