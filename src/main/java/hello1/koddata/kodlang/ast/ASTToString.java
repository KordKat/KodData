package hello1.koddata.kodlang.ast;

import hello1.koddata.utils.collection.ImmutableArray;

public class ASTToString {

    public static String astToString(Statement statement){
        if(statement == null){
            return "-NULL STATEMENT";
        }
        if(statement instanceof ApplyStatement s){
            String st = "|- Apply Statement\n";
            Expression pipe = s.pipeline;
            Expression expr = s.expression;
            st += "   |- Pipe \n";
            st += "     " + astToString(pipe) + "\n";
            st += "   |- Expression \n";
            st += "     " + astToString(expr) + "\n";
            return st;
        }else if(statement instanceof AssignmentExpression s){
            String st = "|- Assignment Expression\n";
            Expression left = s.left;
            Expression right = s.right;
            st += "   |- Left \n";
            st += "      " + astToString(left);
            st += "   |- Right \n";
            st += "      " + astToString(right);
            return st;
        }else if(statement instanceof ArrayLiteral s){
            String st = "|- Array Literal\n";
            ImmutableArray<Expression> lit = s.literals;
            st += "   |- Elements \n";
            for(Expression ex : lit){
                st += "      " + astToString(ex);
            }
            return st;
        }else if(statement instanceof BinaryExpression s){
            String st = "|- Binary Expression\n";
            BinaryExpression.Operator op = s.op;
            Expression left = s.left;
            Expression right = s.right;
            st += "   |- Operator = " + op;
            st += "   |- Left \n";
            st += "      " + astToString(left);
            st += "   |- Right \n";
            st += "      " + astToString(right);
            return st;
        }else if(statement instanceof BlockStatement s){
            String st = "|- Block\n";
            ImmutableArray<Statement> statements = s.statements;
            st += "   |- Statements \n";
            for(Statement ex : statements){
                st += "      " + astToString(ex);
            }
            return st;
        }else if(statement instanceof BooleanLiteral s){
            return "|- Boolean Literal\n   |- value = " + (s.literal ? "True" : "False") + "\n";
        }else if(statement instanceof BranchPipeline s){
            String st = "|- Branch\n";
            ImmutableArray<WhenCaseStatement> whenCase = s.whens;
            ElseCaseStatement elseCase = s.elseCase;
            st += "   |- Cases\n";
            for(WhenCaseStatement wc : whenCase){
                st += "      " + astToString(wc);
            }
            st += "      " + astToString(elseCase);
            return st;
        }else if(statement instanceof DatabaseConnectExpression s){

        }

        return "UNKNOWN";
    }

}
