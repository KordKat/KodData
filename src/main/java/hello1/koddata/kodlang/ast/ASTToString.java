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

        Expression expression = (statement instanceof Expression) ? (Expression) statement : null;

        if(statement instanceof ApplyStatement s){
            String st = linePrefix + "Apply Statement\n";
            st += nextIndent + "|- Pipe \n";
            st += astToStringWithIndent(s.pipeline, nextIndent + "  ");
            st += nextIndent + "|- Expression \n";
            st += astToStringWithIndent(s.expression, nextIndent + "  ");
            return st;
        }else if(statement instanceof AssignmentExpression s){
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
        else if(statement instanceof DatabaseConnectExpression s){
            String st = linePrefix + "Database Connect Expression\n";
            st += nextIndent + "|- Database Type \n";
            st += astToStringWithIndent(s.databaseT, nextIndent + "  ");
            st += nextIndent + "|- Database Name \n";
            st += astToStringWithIndent(s.databaseName, nextIndent + "  ");
            st += nextIndent + "|- User \n";
            st += astToStringWithIndent(s.user, nextIndent + "  ");
            st += nextIndent + "|- Password \n";
            st += astToStringWithIndent(s.pass, nextIndent + "  ");
            st += nextIndent + "|- Port \n";
            st += astToStringWithIndent(s.port, nextIndent + "  ");
            return st;
        }
        else if(statement instanceof DataFrameDeclaration s){
            String st = linePrefix + "DataFrame Declaration\n";
            st += nextIndent + "|- DataFrame Source \n";
            st += astToStringWithIndent(s.dataframe, nextIndent + "  ");
            st += nextIndent + "|- Rename Expression \n";
            st += astToStringWithIndent(s.rename, nextIndent + "  ");
            return st;
        }
        else if(statement instanceof DeleteStatement s){
            String st = linePrefix + "Delete Statement\n";
            st += nextIndent + "|- Target \n";
            st += astToStringWithIndent(s.del, nextIndent + "  ");
            return st;
        }
        else if(statement instanceof DownloadStatement s){
            String st = linePrefix + "Download Statement\n";
            st += nextIndent + "|- Source \n";
            st += astToStringWithIndent(s.src, nextIndent + "  ");
            return st;
        }
        else if(statement instanceof ElseCaseStatement s){
            String st = linePrefix + "Else Case\n";
            st += nextIndent + "|- Do Pipe \n";
            st += astToStringWithIndent(s.doPipe, nextIndent + "  ");
            return st;
        }
        else if(statement instanceof FetchExpression s){
            String st = linePrefix + "Fetch Expression\n";
            st += nextIndent + "|- Data Source = " + s.dataSource + "\n";
            st += nextIndent + "|- Fetch Source \n";
            st += astToStringWithIndent(s.fetchSource, nextIndent + "  ");
            st += nextIndent + "|- Query String \n";
            st += astToStringWithIndent(s.queryString, nextIndent + "  ");
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
        else if(statement instanceof JoinExpression s){
            String st = linePrefix + "Join Expression\n";
            st += nextIndent + "|- Join Type = " + s.joinType + "\n";
            st += nextIndent + "|- DataFrame Declaration \n";
            st += astToStringWithIndent(s.df, nextIndent + "  ");
            st += nextIndent + "|- Left Join On \n";
            st += astToStringWithIndent(s.leftOn, nextIndent + "  ");
            st += nextIndent + "|- Right Join On \n";
            st += astToStringWithIndent(s.rightOn, nextIndent + "  ");
            return st;
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
        else if(statement instanceof ProjectionExpression s){
            String st = linePrefix + "Projection Expression\n";
            st += nextIndent + "|- Projection Source \n";
            st += astToStringWithIndent(s.proj, nextIndent + "  ");
            st += nextIndent + "|- Rename Expression \n";
            st += astToStringWithIndent(s.rename, nextIndent + "  ");
            return st;
        }
        else if(statement instanceof PropertyAccessExpression s){
            String st = linePrefix + "Property Access Expression\n";
            st += nextIndent + "|- Object \n";
            st += astToStringWithIndent(s.object, nextIndent + "  ");
            st += nextIndent + "|- Property \n";
            st += astToStringWithIndent(s.property, nextIndent + "  ");
            return st;
        }
        else if(statement instanceof SelectStatement s){
            String st = linePrefix + "Select Statement\n";
            st += nextIndent + "|- Projections \n";
            for(ProjectionExpression ex : s.projection){
                st += astToStringWithIndent(ex, nextIndent + "  ");
            }
            st += nextIndent + "|- From Source \n";
            st += astToStringWithIndent(s.fromSource, nextIndent + "  ");
            if(s.joinExpressions != null && s.joinExpressions.length() != 0) {
                st += nextIndent + "|- Joins \n";
                for (JoinExpression ex : s.joinExpressions) {
                    st += astToStringWithIndent(ex, nextIndent + "  ");
                }
            }
            if(s.whereClause != null) {
                st += nextIndent + "|- Where Clause \n";
                st += astToStringWithIndent(s.whereClause, nextIndent + "  ");
            }
            if(s.groupByClause != null) {
                st += nextIndent + "|- Group By Clause \n";
                st += astToStringWithIndent(s.groupByClause, nextIndent + "  ");
            }
            if(s.havingClause != null) {
                st += nextIndent + "|- Having Clause \n";
                st += astToStringWithIndent(s.havingClause, nextIndent + "  ");
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