package hello1.koddata.kodlang.ast;

public class AssignmentExpression extends Expression {

    public final Expression right;
    public final Expression left;

    public AssignmentExpression(Expression left, Expression right) {
        super(StatementType.ASSIGNMENT);
        this.left = left;
        this.right = right;
    }
}
