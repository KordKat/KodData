package hello1.koddata.kodlang.ast;

//Inheritance
public sealed class BranchMember extends Expression permits WhenCaseStatement, ElseCaseStatement {

    public BranchMember(){
        super(StatementType.BRANCH_MEMBER);
    }

}
