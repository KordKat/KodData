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
        }
    }

    private static ReturnType evaluateExpression(Expression expression){
        return ReturnType.NULL;
    }

}
