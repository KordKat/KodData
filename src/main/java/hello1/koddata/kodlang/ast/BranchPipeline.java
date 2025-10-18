package hello1.koddata.kodlang.ast;

import hello1.koddata.utils.collection.ImmutableArray;

public class BranchPipeline extends Pipeline {

    final ImmutableArray<WhenCaseStatement> whens;
    final ElseCaseStatement elseCase;

    public BranchPipeline(ImmutableArray<WhenCaseStatement> whens, ElseCaseStatement elseCase) {
        super(StatementType.BRANCH);
        this.whens = whens;
        this.elseCase = elseCase;
    }
}
