package hello1.koddata.kodlang.ast;

public class NumberLiteral extends Expression {

    final char[] literal;
    final boolean isFloat;

    public NumberLiteral(char[] literal, boolean isFloat){
        super(StatementType.NUMBER_LITERAL);
        this.literal = literal;
        this.isFloat = isFloat;
    }

}
