package hello1.koddata.kodlang.ast;

import hello1.koddata.utils.collection.ImmutableArray;

public class FunctionCall extends Expression {

    public final NIdentifier function;
    public final ImmutableArray<Expression> arguments;

    public FunctionCall(NIdentifier function, ImmutableArray<Expression> arguments){
        super(StatementType.FUNCTION);
        this.function = function;
        this.arguments = arguments;
    }

}
