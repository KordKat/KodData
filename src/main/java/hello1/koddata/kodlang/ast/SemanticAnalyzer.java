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
            }else if(op.equals(BinaryExpression.Operator.ADD)){
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.NUMBER) || !r2.equals(ReturnType.NUMBER)){
                    throw new KException(ExceptionCode.KDC0003,"Only number can perform adding");
                }
            }else if(op.equals(BinaryExpression.Operator.SUB)){
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.NUMBER) || !r2.equals(ReturnType.NUMBER)){
                    throw new KException(ExceptionCode.KDC0003,"Only number can perform subtracting");
                }
            }else if(op.equals(BinaryExpression.Operator.MUL)){
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.NUMBER) || !r2.equals(ReturnType.NUMBER)){
                    throw new KException(ExceptionCode.KDC0003,"Only number can perform multiple");
                }
            }else if(op.equals(BinaryExpression.Operator.POWER)){
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.NUMBER) || !r2.equals(ReturnType.NUMBER)){
                    throw new KException(ExceptionCode.KDC0003,"Only number can perform power");
                }
            }else if(op.equals(BinaryExpression.Operator.GREATER)){            //greater to not equal----------------------------------
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.STRING) && !r2.equals(ReturnType.STRING) || !r1.equals(ReturnType.NUMBER) && !r2.equals(ReturnType.NUMBER)){
                    throw new KException(ExceptionCode.KDC0003,"Only string , number can perform greater");
                }
            }else if(op.equals(BinaryExpression.Operator.GREATEREQ)){
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.STRING) && !r2.equals(ReturnType.STRING) || !r1.equals(ReturnType.NUMBER) && !r2.equals(ReturnType.NUMBER)){
                    throw new KException(ExceptionCode.KDC0003,"Only string , number can perform greater equal");
                }
            }else if(op.equals(BinaryExpression.Operator.LESSTHAN)){
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.STRING) && !r2.equals(ReturnType.STRING) || !r1.equals(ReturnType.NUMBER) && !r2.equals(ReturnType.NUMBER)){
                    throw new KException(ExceptionCode.KDC0003,"Only string , number can perform less than");
                }
            }else if(op.equals(BinaryExpression.Operator.LESSTHANEQ)){
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.STRING) && !r2.equals(ReturnType.STRING) || !r1.equals(ReturnType.NUMBER) && !r2.equals(ReturnType.NUMBER)){
                    throw new KException(ExceptionCode.KDC0003,"Only string , number can perform less than equal");
                }
            } else if (op.equals(BinaryExpression.Operator.EQUALS)) {
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (r1 != r2){
                    throw new KException(ExceptionCode.KDC0003,"Only same type can perform equal");
                }

            } else if (op.equals(BinaryExpression.Operator.NEQUALS)) {
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (r1 != r2){
                    throw new KException(ExceptionCode.KDC0003,"Only same type can perform not equal");
                }

            }else if (op.equals(BinaryExpression.Operator.IN)) {            //In----------------------------------
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.NUMBER) && !r2.equals(ReturnType.ARRAY)){
                    throw new KException(ExceptionCode.KDC0003,"only use number(on right) and array(on left) to use in");
                }
            } else if (op.equals(BinaryExpression.Operator.AND)) {            //And----------------------------------
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.LOGICAL) && !r2.equals(ReturnType.LOGICAL)){
                    throw new KException(ExceptionCode.KDC0003,"Only logical type can perform not and");
                }
            }else if (op.equals(BinaryExpression.Operator.OR)) {            //Or----------------------------------
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.LOGICAL) && !r2.equals(ReturnType.LOGICAL)){
                    throw new KException(ExceptionCode.KDC0003,"Only logical type can perform not or");
                }
            }


        }
//        else if (statement instanceof DatabaseConnectExpression databaseConnectExpression){
//            StringLiteral databaseT = databaseConnectExpression.databaseT;
//            StringLiteral databaseName = databaseConnectExpression.databaseName;
//            StringLiteral user = databaseConnectExpression.user;
//            StringLiteral pass = databaseConnectExpression.pass;
//            NumberLiteral port = databaseConnectExpression.port;
//            if (){
//
//            }
//        }
    }

    private static ReturnType evaluateExpression(Expression expression){
        return ReturnType.NULL;
    }

}
