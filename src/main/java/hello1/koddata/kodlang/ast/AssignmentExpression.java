package hello1.koddata.kodlang.ast;

public class AssignmentExpression extends Expression {

    final Expression right;
    final Expression left;

    public AssignmentExpression(Expression left, Expression right) {
        super(StatementType.ASSIGNMENT);
        this.left = left;
        this.right = right;
    }
}
