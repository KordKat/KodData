package hello1.koddata.engine;

import java.util.List;
import java.util.Set;

// stored as direct acyclic graph
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
}

