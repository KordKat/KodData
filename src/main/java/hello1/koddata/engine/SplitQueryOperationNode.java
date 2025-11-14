package hello1.koddata.engine;

import java.util.List;
import java.util.function.Predicate;

public class SplitQueryOperationNode extends QueryOperationNode {

    private List<QueryOperationNode> nextNode;
    private List<Predicate<Value<?>>> nodeSelector;
    public SplitQueryOperationNode() {
        super(new SplitOperation());
    }

    @Override
    public void next(QueryOperationNode nextNode) {
        this.nextNode.add(nextNode);
    }
}