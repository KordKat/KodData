package hello1.koddata.engine;

import hello1.koddata.exception.KException;


public class CopyOperation implements QueryOperation {

    // เก็บชื่อ column ที่รับเข้ามา
    private final String columnName;
    public CopyOperation(String columnName) {
        this.columnName = columnName; // รับผ่าน constructor ตรงนี้
    }

    @Override
    public Value<?> operate(Value<?> value) throws KException {
        // คืนชื่อ column กลับไปให้ engine ใช้ในการ copy
        return new Value<>(columnName);
    }
}