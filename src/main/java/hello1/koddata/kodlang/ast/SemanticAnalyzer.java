package hello1.koddata.kodlang.ast;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.utils.collection.ImmutableArray;

import java.sql.SQLData;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

public class SemanticAnalyzer {
//    NumberLiteral AssignmentExpression Subscript BlockStatement BinaryExpression

    public void analyze(Statement statement) throws KException {
        if(statement instanceof NumberLiteral num){
            String value = new String(num.literal);
            if(num.isFloat){
                try{
                    Float.parseFloat(value);
                }catch (NumberFormatException nfx){
                    throw new KException(ExceptionCode.KDC0003, "Cannot parse " + value + " to float");
                }
            }else {
                try{
                    Long.parseLong(value);
                }catch (NumberFormatException nfx){
                    throw new KException(ExceptionCode.KDC0003, "Cannot parse " + value + " to integer");
                }
            }
        }else if(statement instanceof BlockStatement block){
            for(Statement statement1 : block.statements){
                analyze(statement1);
            }
        }else if (statement instanceof BinaryExpression bin){
            Expression left = bin.left;
            Expression right = bin.right;
            BinaryExpression.Operator op = bin.op;
            analyze(left);
            analyze(right);
        }
        else if (statement instanceof AssignmentExpression assignmentExpression){
//            left = identifier or subscript
//            subscript base = identifier
            Expression left = assignmentExpression.left;
            Expression right = assignmentExpression.right;

            if(!left.type.equals(Statement.StatementType.IDENTIFIER) && !left.type.equals(Statement.StatementType.SUBSCRIPT)){
                throw new KException(ExceptionCode.KDC0001,"in left only identifier and subscript allow");
            }
            if(left.type.equals(Statement.StatementType.IDENTIFIER)){
                String identifier = ((Identifier)left).identifier;
            }
            else{
                this.analyze(left);
                this.analyze(right);
                String identifier = ((Identifier)((Subscript)left).base).identifier;
            }


        }

        else if (statement instanceof Subscript subscript){
            Expression base = subscript.base;
            Expression index = subscript.index;

            if(!base.type.equals(Statement.StatementType.IDENTIFIER)){
                throw new KException(ExceptionCode.KDC0001,"only identifier allow in base");
            }
            analyze(index);
        }

        else if (statement instanceof ApplyStatement applyStatement){
            Expression expression = applyStatement.expression;
            Expression pipeline = applyStatement.pipeline;
            analyze(expression);
            analyze(pipeline);
        }

        else if (statement instanceof ArrayLiteral arrayLiteral){
            ImmutableArray<Expression> literals = arrayLiteral.literals;
            for (Expression e : literals){
                analyze(e);
            }
        }

//        ------------------------
        else if (statement instanceof BranchPipeline branchPipeline){
            ImmutableArray<WhenCaseStatement> whens = branchPipeline.whens;
            ElseCaseStatement elseCase = branchPipeline.elseCase;
            analyze(elseCase);
            for (WhenCaseStatement e : whens){
                analyze(e);
            }
        }

        else if (statement instanceof DeleteStatement deleteStatement){
            Expression del = deleteStatement.del;
            analyze(del);
        }

        else if (statement instanceof DownloadStatement downloadStatement){
            Expression downloadStatemen = downloadStatement.src;
            analyze(downloadStatemen);
        }

        else if (statement instanceof ElseCaseStatement elseCaseStatement){
            Expression doPipe = elseCaseStatement.doPipe;
            analyze(doPipe);
        }

        else if(statement instanceof FunctionCall functionCall){
            NIdentifier function = functionCall.function;
            ImmutableArray<Expression> arguments = functionCall.arguments;
            analyze(function);
            for (Expression e : arguments){
                analyze(e);
            }
        }

        else if(statement instanceof Pipeline pipeline){
            ImmutableArray<Expression> pipe = pipeline.pipeline;
            for (Expression e : pipe){
                if(e instanceof NIdentifier || e.type.equals(Statement.StatementType.BRANCH)){
                    analyze(e);
                }
                else {
                    throw new KException(ExceptionCode.KDC0003,"Pineline can only use NIdentifier or Branch");
                }
            }

        }

        else if(statement instanceof PropertyAccessExpression propertyAccessExpression){
            Expression object = propertyAccessExpression.object;
            analyze(object);
        }

        else if(statement instanceof UnaryExpression unaryExpression){
            Expression expression = unaryExpression.expression;
            analyze(expression);
        }

        else if(statement instanceof WhenCaseStatement whenCaseStatement ){
            Expression condition = whenCaseStatement.condition;
            Expression doPipe = whenCaseStatement.doPipe;
            analyze(condition);
            analyze(doPipe);
        }

    }

    private static NIdentifier getNIdentifier(DatabaseConnectExpression databaseConnectExpression) throws KException {
        NIdentifier databaseT = databaseConnectExpression.databaseT;
        StringLiteral databaseName = databaseConnectExpression.databaseName;
        StringLiteral user = databaseConnectExpression.user;
        StringLiteral pass = databaseConnectExpression.pass;
        NumberLiteral port = databaseConnectExpression.port;
        StringLiteral host = databaseConnectExpression.host;


        if(databaseT == null || databaseName == null || user == null || pass == null || port == null || host == null){
            throw new KException(ExceptionCode.KDC0001,"some value is null");
        }
        return databaseT;
    }

}
