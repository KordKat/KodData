package hello1.koddata.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public class SplitQueryOperationNode extends QueryOperationNode {

    private List<QueryOperationNode> nextNode = new ArrayList<>();
    private List<Predicate<Value<?>>> nodeSelector =  new ArrayList<>();
    public SplitQueryOperationNode() {
        super(new SplitOperation());
    }

    @Override
    public void next(QueryOperationNode nextNode) {
        this.nextNode.add(nextNode);
    }

    public void addNext(Predicate<Value<?>> selector, QueryOperationNode next){
        nodeSelector.add(selector);
        nextNode.add(next);
    }

    public QueryOperationNode selectNext(Value<?> value){
        for(int i = 0; i < nodeSelector.size(); i++ )
            if(nodeSelector.get(i).test(value)){
                return nextNode.get(i);
            }
        return null;
    }
}