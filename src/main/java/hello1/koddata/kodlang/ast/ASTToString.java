package hello1.koddata.kodlang.ast;

import hello1.koddata.utils.collection.ImmutableArray;

public class ASTToString {

    public static String astToString(Statement statement){
        return astToStringWithIndent(statement, "");
    }

    private static String indentString(String text, String indent) {
        if (text == null || text.isEmpty()) {
            return "";
        }
        return indent + text.replaceAll("\\n", "\n" + indent);
    }

    private static String astToStringWithIndent(Statement statement, String currentIndent){

        final String nextIndent = currentIndent + "  ";

        final String linePrefix = currentIndent + "|- ";

        if(statement == null){
            return currentIndent + "-NULL STATEMENT\n";
        }

        if(statement instanceof AssignmentExpression s){
            String st = linePrefix + "Assignment Expression\n";
            st += nextIndent + "|- Left \n";
            st += astToStringWithIndent(s.left, nextIndent + "  ");
            st += nextIndent + "|- Right \n";
            st += astToStringWithIndent(s.right, nextIndent + "  ");
            return st;
        }else if(statement instanceof ArrayLiteral s){
            String st = linePrefix + "Array Literal\n";
            st += nextIndent + "|- Elements \n";
            for(Expression ex : s.literals){
                st += astToStringWithIndent(ex, nextIndent + "  ");
            }
            return st;
        }else if(statement instanceof BinaryExpression s){
            String st = linePrefix + "Binary Expression\n";
            st += nextIndent + "|- Operator = " + s.op + "\n";
            st += nextIndent + "|- Left \n";
            st += astToStringWithIndent(s.left, nextIndent + "  ");
            st += nextIndent + "|- Right \n";
            st += astToStringWithIndent(s.right, nextIndent + "  ");
            return st;
        }else if(statement instanceof BlockStatement s){
            String st = linePrefix + "Block\n";
            st += nextIndent + "|- Statements \n";
            for(Statement ex : s.statements){
                st += astToStringWithIndent(ex, nextIndent + "  ");
            }
            return st;
        }else if(statement instanceof BooleanLiteral s){
            return linePrefix + "Boolean Literal\n" + nextIndent + "|- value = " + (s.literal ? "True" : "False") + "\n";
        }else if(statement instanceof BranchPipeline s){
            String st = linePrefix + "Branch\n";
            st += nextIndent + "|- Cases\n";
            for(WhenCaseStatement wc : s.whens){
                st += astToStringWithIndent(wc, nextIndent + "  ");
            }
            st += astToStringWithIndent(s.elseCase, nextIndent + "  ");
            return st;
        }
        else if(statement instanceof ElseCaseStatement s){
            String st = linePrefix + "Else Case\n";
            st += nextIndent + "|- Do Pipe \n";
            st += astToStringWithIndent(s.doPipe, nextIndent + "  ");
            return st;
        }
        else if(statement instanceof FunctionCall s){
            String st = linePrefix + "Function Call\n";
            st += nextIndent + "|- Function \n";
            st += astToStringWithIndent(s.function, nextIndent + "  ");
            st += nextIndent + "|- Arguments \n";
            for(Expression arg : s.arguments){
                st += astToStringWithIndent(arg, nextIndent + "  ");
            }
            return st;
        }
        else if(statement instanceof NIdentifier s){
            return linePrefix + "Named Identifier\n" + nextIndent + "|- name = " + s.identifier + "\n";
        }
        else if(statement instanceof Identifier s){
            return linePrefix + "Identifier\n" + nextIndent + "|- name = " + s.identifier + "\n";
        }
        else if(statement instanceof NullLiteral){
            return linePrefix + "Null Literal\n";
        }
        else if(statement instanceof NumberLiteral s){
            return linePrefix + "Number Literal\n" + nextIndent + "|- value = " + new String(s.literal) + (s.isFloat ? " (Float)" : " (Integer)") + "\n";
        }
        else if(statement instanceof Pipeline s){
            String st = linePrefix + "Pipeline\n";
            st += nextIndent + "|- Steps \n";
            for(Expression ex : s.pipeline){
                st += astToStringWithIndent(ex, nextIndent + "  ");
            }
            return st;
        }
        else if(statement instanceof StringLiteral s){
            return linePrefix + "String Literal\n" + nextIndent + "|- value = \"" + new String(s.literal) + "\"\n";
        }
        else if(statement instanceof Subscript s){
            String st = linePrefix + "Subscript Access\n";
            st += nextIndent + "|- Base \n";
            st += astToStringWithIndent(s.base, nextIndent + "  ");
            st += nextIndent + "|- Index \n";
            st += astToStringWithIndent(s.index, nextIndent + "  ");
            return st;
        }
        else if(statement instanceof UnaryExpression s){
            String st = linePrefix + "Unary Expression\n";
            st += nextIndent + "|- Operator = " + s.op + "\n";
            st += nextIndent + "|- Expression \n";
            st += astToStringWithIndent(s.expression, nextIndent + "  ");
            return st;
        }
        else if(statement instanceof WhenCaseStatement s){
            String st = linePrefix + "When Case\n";
            st += nextIndent + "|- Condition \n";
            st += astToStringWithIndent(s.condition, nextIndent + "  ");
            st += nextIndent + "|- Do Pipe \n";
            st += astToStringWithIndent(s.doPipe, nextIndent + "  ");
            return st;
        }

        return currentIndent + "UNKNOWN\n";
    }
}