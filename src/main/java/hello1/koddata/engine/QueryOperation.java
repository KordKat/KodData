package hello1.koddata.engine;

import hello1.koddata.exception.KException;
// Abstract
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ
public interface QueryOperation {

    Value<?> operate(Value<?> value) throws KException;

}
