package hello1.koddata.kodlang.ast;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.sql.SQLData;
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

    public static ReturnType analyze(Statement statement) throws KException {
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
            return ReturnType.NUMBER;
        }else if(statement instanceof BlockStatement block){
            for(Statement statement1 : block.statements){
                analyze(statement1);
            }
            return ReturnType.NULL;
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
                return ReturnType.NUMBER;
            }else if(op.equals(BinaryExpression.Operator.ADD)){
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
//                if (!r1.equals(ReturnType.NUMBER) || !r2.equals(ReturnType.NUMBER)){
//                    throw new KException(ExceptionCode.KDC0003,"Only number can perform adding");
//                }
//                return ReturnType.NUMBER;
                if (r1.equals(ReturnType.NUMBER) && r2.equals(ReturnType.NUMBER)){
                    return ReturnType.NUMBER;
                }else if(r1.equals(ReturnType.STRING) && r2.equals(ReturnType.STRING)){
                    return ReturnType.STRING;
                } else if (r1.equals(ReturnType.NUMBER) && r2.equals(ReturnType.STRING) || r1.equals(ReturnType.STRING) && r2.equals(ReturnType.NUMBER)) {
                    return ReturnType.STRING;
                }else if (r1.equals(ReturnType.LOGICAL) && r2.equals(ReturnType.STRING) || r1.equals(ReturnType.STRING) && r2.equals(ReturnType.LOGICAL)) {
                    return ReturnType.STRING;
                }else{
                    throw new KException(ExceptionCode.KDC0003,"use number parsing wrong");
                }

            }else if(op.equals(BinaryExpression.Operator.SUB)){
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.NUMBER) || !r2.equals(ReturnType.NUMBER)){
                    throw new KException(ExceptionCode.KDC0003,"Only number can perform subtracting");
                }
                return ReturnType.NUMBER;
            }else if(op.equals(BinaryExpression.Operator.MUL)){
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.NUMBER) || !r2.equals(ReturnType.NUMBER)){
                    throw new KException(ExceptionCode.KDC0003,"Only number can perform multiple");
                }
                return ReturnType.NUMBER;
            }else if(op.equals(BinaryExpression.Operator.POWER)){
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.NUMBER) || !r2.equals(ReturnType.NUMBER)){
                    throw new KException(ExceptionCode.KDC0003,"Only number can perform power");
                }
                return ReturnType.NUMBER;
            }else if(op.equals(BinaryExpression.Operator.GREATER)){            //greater to not equal----------------------------------
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.STRING) && !r2.equals(ReturnType.STRING) || !r1.equals(ReturnType.NUMBER) && !r2.equals(ReturnType.NUMBER)){
                    throw new KException(ExceptionCode.KDC0003,"Only string , number can perform greater");
                }
                return ReturnType.LOGICAL;
            }else if(op.equals(BinaryExpression.Operator.GREATEREQ)){
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.STRING) && !r2.equals(ReturnType.STRING) || !r1.equals(ReturnType.NUMBER) && !r2.equals(ReturnType.NUMBER)){
                    throw new KException(ExceptionCode.KDC0003,"Only string , number can perform greater equal");
                }
                return ReturnType.LOGICAL;
            }else if(op.equals(BinaryExpression.Operator.LESSTHAN)){
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.STRING) && !r2.equals(ReturnType.STRING) || !r1.equals(ReturnType.NUMBER) && !r2.equals(ReturnType.NUMBER)){
                    throw new KException(ExceptionCode.KDC0003,"Only string , number can perform less than");
                }
                return ReturnType.LOGICAL;
            }else if(op.equals(BinaryExpression.Operator.LESSTHANEQ)){
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.STRING) && !r2.equals(ReturnType.STRING) || !r1.equals(ReturnType.NUMBER) && !r2.equals(ReturnType.NUMBER)){
                    throw new KException(ExceptionCode.KDC0003,"Only string , number can perform less than equal");
                }
                return ReturnType.LOGICAL;
            } else if (op.equals(BinaryExpression.Operator.EQUALS)) {
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (r1 != r2){
                    throw new KException(ExceptionCode.KDC0003,"Only same type can perform equal");
                }
                return ReturnType.LOGICAL;
            } else if (op.equals(BinaryExpression.Operator.NEQUALS)) {
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (r1 != r2){
                    throw new KException(ExceptionCode.KDC0003,"Only same type can perform not equal");
                }
                return ReturnType.LOGICAL;
            }else if (op.equals(BinaryExpression.Operator.IN)) {            //In----------------------------------
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.NUMBER) && !r2.equals(ReturnType.ARRAY)){
                    throw new KException(ExceptionCode.KDC0003,"only use number(on right) and array(on left) to use in");
                }
                return ReturnType.LOGICAL;
            } else if (op.equals(BinaryExpression.Operator.AND)) {            //And----------------------------------
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.LOGICAL) && !r2.equals(ReturnType.LOGICAL)){
                    throw new KException(ExceptionCode.KDC0003,"Only logical type can perform and");
                }
                return ReturnType.LOGICAL;
            }else if (op.equals(BinaryExpression.Operator.OR)) {            //Or----------------------------------
                ReturnType r1 = evaluateExpression(left);
                ReturnType r2 = evaluateExpression(right);
                if (!r1.equals(ReturnType.LOGICAL) && !r2.equals(ReturnType.LOGICAL)){
                    throw new KException(ExceptionCode.KDC0003,"Only logical type can perform or");
                }
                return ReturnType.LOGICAL;
            }


        }
        else if (statement instanceof DatabaseConnectExpression databaseConnectExpression){
            NIdentifier databaseT = databaseConnectExpression.databaseT;
            StringLiteral databaseName = databaseConnectExpression.databaseName;
            StringLiteral user = databaseConnectExpression.user;
            StringLiteral pass = databaseConnectExpression.pass;
            NumberLiteral port = databaseConnectExpression.port;
            StringLiteral host = databaseConnectExpression.host;

            ReturnType dataBName = evaluateExpression(databaseName);
            ReturnType u = evaluateExpression(user);
            ReturnType pa = evaluateExpression(pass);
            ReturnType po = evaluateExpression(port);
            ReturnType ho = evaluateExpression(host);

            if(databaseT == null || databaseName == null || user == null || pass == null || port == null || host == null){
                throw new KException(ExceptionCode.KDC0001,"some value is null");
            }

            if (!dataBName.equals(ReturnType.STRING) ){
                throw new KException(ExceptionCode.KDC0003,"database name only use string");
            }
            if(!u.equals(ReturnType.STRING)){
                throw new KException(ExceptionCode.KDC0003,"user only use string");
            }
            if(!pa.equals(ReturnType.STRING)){
                throw new KException(ExceptionCode.KDC0003,"password only use string");
            }
            if(!ho.equals(ReturnType.STRING)){
                throw new KException(ExceptionCode.KDC0003,"host only use string");
            }
            if(!po.equals(ReturnType.NUMBER)){
                throw new KException(ExceptionCode.KDC0003,"port only use string");
            }
            if (databaseT.identifier.equalsIgnoreCase("mysql") || databaseT.identifier.equalsIgnoreCase("cassandra") || databaseT.identifier.equalsIgnoreCase("mariadb")){
                return ReturnType.CONNECTION;
            }else {
                throw new KException(ExceptionCode.KDC0003,"database only use mysql , cassandra or mariadb");
            }

        }

        else if (statement instanceof  JoinExpression joinExpression){
            DataFrameDeclaration df = joinExpression.df;
            Expression leftOn = joinExpression.leftOn;
            Expression rightOn = joinExpression.rightOn;
            JoinExpression.JoinType jT = joinExpression.joinType;
            SemanticAnalyzer.analyze(df);

//            if (jT.equals(JoinExpression.JoinType.INNER)){
//                ReturnType left = evaluateExpression(leftOn);
//                ReturnType right = evaluateExpression(rightOn);
//                if(){
//
//                }
//            }

        }

        return ReturnType.NULL;
    }

    private static ReturnType evaluateExpression(Expression expression){
        return ReturnType.NULL;
    }

}
