package hello1.koddata.kodlang.ast;

public class PropertyAccessExpression extends Expression {
    final Expression object;
    final Identifier property;
    public PropertyAccessExpression(Expression object, Identifier property){
        super(StatementType.ACCESS);
        this.object = object;
        this.property = property;
    }
}
