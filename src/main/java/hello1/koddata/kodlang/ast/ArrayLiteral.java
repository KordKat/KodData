package hello1.koddata.kodlang.ast;

import hello1.koddata.utils.collection.ImmutableArray;

//Inheritance
public class ArrayLiteral extends Expression {

    public final ImmutableArray<Expression> literals;
    public ArrayLiteral(ImmutableArray<Expression> literals){
        super(StatementType.ARRAY_LITERAL);
        this.literals = literals;
    }

}
