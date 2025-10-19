package hello1.koddata.kodlang.ast;

import hello1.koddata.utils.collection.ImmutableArray;

public class FunctionCall extends Expression {

    final NIdentifier function;
    final ImmutableArray<Expression> arguments;

    public FunctionCall(NIdentifier function, ImmutableArray<Expression> arguments){
        super(StatementType.FUNCTION);
        this.function = function;
        this.arguments = arguments;
    }

}
