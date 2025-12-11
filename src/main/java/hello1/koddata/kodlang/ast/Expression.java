package hello1.koddata.kodlang.ast;

//Inheritance
public abstract class Expression extends Statement{
    public Expression(StatementType type) {
        super(type);
    }
}
