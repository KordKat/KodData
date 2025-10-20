package hello1.koddata.kodlang.ast;

public non-sealed class ElseCaseStatement extends BranchMember {

    final Expression doPipe;

    public ElseCaseStatement(Expression doPipe){
        this.doPipe = doPipe;
    }

}
