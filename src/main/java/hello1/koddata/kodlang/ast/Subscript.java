package hello1.koddata.kodlang.ast;

public class Subscript extends Expression {

    final Expression base;
    final Expression index;

    public Subscript(Expression base, Expression index){
        super(StatementType.SUBSCRIPT);
        this.base = base;
        this.index = index;
    }

}
