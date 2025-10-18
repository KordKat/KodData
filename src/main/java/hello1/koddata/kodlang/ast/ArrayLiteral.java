package hello1.koddata.kodlang.ast;

import hello1.koddata.utils.collection.ImmutableArray;

public class ArrayLiteral extends Expression {

    final ImmutableArray<String> literals;
    public ArrayLiteral(ImmutableArray<String> literals){
        super(StatementType.ARRAY_LITERAL);
        this.literals = literals;
    }

}
