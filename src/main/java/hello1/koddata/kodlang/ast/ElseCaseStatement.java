package hello1.koddata.kodlang.ast;

public non-sealed class ElseCaseStatement extends BranchMember {

    final Pipeline doPipe;

    public ElseCaseStatement(Pipeline doPipe){
        this.doPipe = doPipe;
    }

}
