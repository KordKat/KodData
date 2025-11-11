package hello1.koddata.engine;

import hello1.koddata.sessions.Session;

public class QueryExecution {
    private final QueryOperationNode head = new QueryOperationNode(new EmptyOperation());

    public QueryExecution() {}

    public QueryOperationNode getHead() {
        return head;
    }

    public void execute(Session session){}
}
