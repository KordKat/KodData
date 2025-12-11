package hello1.koddata.kodlang.ast;

//Inheritance
public class Subscript extends Expression {

    public final Expression base;
    public final Expression index;

    public Subscript(Expression base, Expression index){
        super(StatementType.SUBSCRIPT);
        this.base = base;
        this.index = index;
    }

}
