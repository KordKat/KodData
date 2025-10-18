package hello1.koddata.kodlang.ast;

public class DeleteStatement extends Statement {

    final Expression del;

    public DeleteStatement(Expression del){
        super(StatementType.DELETE);
        this.del = del;
    }

}
