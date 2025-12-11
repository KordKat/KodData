package hello1.koddata.engine;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.util.List;
import java.util.Set;

// stored as direct acyclic graph
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ
public class QueryOperationNode {

    private QueryOperation operation;
    private QueryOperationNode nextNode;
    private List<Value<?>>  arguments;
    public QueryOperationNode(QueryOperation operation , List<Value<?>>  arguments){
        this.operation = operation;
        this.arguments = arguments;
    }

    public void next(QueryOperationNode nextNode) {
        this.nextNode = nextNode;
    }

    public QueryOperation getOperation() {
        return operation;
    }

    public QueryOperationNode getNextNode() {
        return nextNode;
    }

    public String getColumn() throws KException {
        Value<?> val = arguments.get(0);
        if(!(val.get() instanceof String name)){
            throw new KException(ExceptionCode.KD00005, "should be string");
        }
        return name;
    }

}

