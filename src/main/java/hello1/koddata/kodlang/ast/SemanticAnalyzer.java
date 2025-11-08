package hello1.koddata.kodlang.ast;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.text.NumberFormat;

public class SemanticAnalyzer {
    enum ReturnType {
        NUMBER,
        STRING,
        PIPELINE,
        LOGICAL,
        CONNECTION,
        ARRAY,
        DATAFRAME,
        NULL
    }

    public static void analyze(Statement statement) throws KException {
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
            if(op.equals(BinaryExpression.Operator.DIV)){
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.NUMBER) || !r2.equals(ReturnType.NUMBER)){
                    throw new KException(ExceptionCode.KDC0003,"Only number can perform division");
                }
            }
        }
    }

    private static ReturnType evaluateExpression(Expression expression){
        return ReturnType.NULL;
    }

}
