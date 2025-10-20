package hello1.koddata.kodlang.ast;

import hello1.koddata.utils.collection.ImmutableArray;

public class Pipeline extends Expression {

    ImmutableArray<Expression> pipeline;

    public Pipeline(ImmutableArray<Expression> pipelineFunctions){
        super(StatementType.PIPELINE_FUNC);
        this.pipeline = pipelineFunctions;
    }

    protected Pipeline(StatementType type){
        super(type);
    }

}
