package hello1.koddata.engine;

import hello1.koddata.sessions.Session;

//Inheritance
public class QueryExecution {
    private final QueryOperationNode head = new QueryOperationNode(new EmptyOperation() , null);

    public QueryExecution() {}

    public QueryOperationNode getHead() {
        return head;
    }

    public void execute(Session session){}
}
