package hello1.koddata.engine;

import java.util.Set;

// stored as direct acyclic graph
public class QueryOperationNode {

    private QueryOperation operation;
    private QueryOperationNode nextNode;
    public QueryOperationNode(QueryOperation operation){

    }

    public void next(QueryOperationNode nextNode) {
        this.nextNode = nextNode;
    }

}
