package hello1.koddata.kodlang.ast;

public class BinaryExpression extends Expression {

    final Expression left;
    final Expression right;

    public BinaryExpression(Expression left, Expression right) {
        super(StatementType.BINARY_EXPR);
        this.left = left;
        this.right = right;
    }

}
