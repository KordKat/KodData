package hello1.koddata.kodlang.ast;

public class UnaryExpression extends Expression {

    final BinaryExpression.Operator op;
    final Expression expression;

    public UnaryExpression(BinaryExpression.Operator op, Expression expression){
        super(StatementType.UNARY_EXPR);
        this.op = op;
        this.expression = expression;
    }

}
