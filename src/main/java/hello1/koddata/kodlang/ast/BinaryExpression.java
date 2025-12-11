package hello1.koddata.kodlang.ast;

//Inheritance
public class BinaryExpression extends Expression {

    public final Expression left;
    public final Expression right;
    public final Operator op;
    public enum Operator {
        ADD,
        SUB,
        MUL,
        DIV,
        POWER,
        AND,
        OR,
        EQUALS,
        NEQUALS,
        IN,
        GREATER,
        GREATEREQ,
        LESSTHAN,
        LESSTHANEQ
    }
    public BinaryExpression(Operator op, Expression left, Expression right) {
        super(StatementType.BINARY_EXPR);
        this.left = left;
        this.right = right;
        this.op = op;
    }

}
