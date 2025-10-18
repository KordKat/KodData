package hello1.koddata.kodlang.ast;

public non-sealed class WhenCaseStatement extends BranchMember {

    final Expression condition;
    final Pipeline doPipe;


    public WhenCaseStatement(Expression condition, Pipeline doPipe){
        this.condition = condition;
        this.doPipe = doPipe;
    }

}
