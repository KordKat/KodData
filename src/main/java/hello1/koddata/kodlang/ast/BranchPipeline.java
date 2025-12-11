package hello1.koddata.kodlang.ast;

import hello1.koddata.utils.collection.ImmutableArray;

//Inheritance
public class BranchPipeline extends Pipeline {

    public final ImmutableArray<WhenCaseStatement> whens;
    public final ElseCaseStatement elseCase;

    public BranchPipeline(ImmutableArray<WhenCaseStatement> whens, ElseCaseStatement elseCase) {
        super(StatementType.BRANCH);
        this.whens = whens;
        this.elseCase = elseCase;
    }
}
