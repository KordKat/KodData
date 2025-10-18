package hello1.koddata.kodlang.ast;

public class AssignmentStatement extends Statement {

    final Expression right;
    final Expression left;

    public AssignmentStatement(Expression left, Expression right) {
        super(StatementType.ASSIGNMENT);
        this.left = left;
        this.right = right;
    }
}
