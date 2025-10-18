package hello1.koddata.kodlang.ast;

public sealed class BranchMember extends Statement permits WhenCaseStatement, ElseCaseStatement {

    public BranchMember(){
        super(StatementType.BRANCH_MEMBER);
    }

}
