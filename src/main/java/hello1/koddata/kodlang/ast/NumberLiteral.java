package hello1.koddata.kodlang.ast;

public class NumberLiteral extends Expression {

    public final char[] literal;
    public final boolean isFloat;

    public NumberLiteral(char[] literal, boolean isFloat){
        super(StatementType.NUMBER_LITERAL);
        this.literal = literal;
        this.isFloat = isFloat;
    }

}
