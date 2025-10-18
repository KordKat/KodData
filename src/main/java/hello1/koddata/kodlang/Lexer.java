package hello1.koddata.kodlang;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Lexer {

    private static final int ANALYZE_SUCCESS = 0;
    private static final int UNKNOWN_TOKEN = 1;

    public static Token[] analyze(char[] code){
        List<Token> tokens = new ArrayList<>();
        int i = 0;
        while(i < code.length){
            char c = code[i];

            if(Character.isWhitespace(c)){
                while(i < code.length && Character.isWhitespace(code[i])) i++;
            }else if(c == '$') { //Identifier
                int start = i++;
                while(i < code.length && (Character.isLetterOrDigit(code[i]) || code[i] == '_')) i++;
                tokens.add(new Token(Token.TokenType.IDENTIFIER, Arrays.copyOfRange(code, start+1, i), start, i));
            }else if(Character.isDigit(c)){ //Number
                int start = i;
                while(i < code.length && Character.isDigit(code[i])) i++;
                tokens.add(new Token(Token.TokenType.NUMBER, Arrays.copyOfRange(code, start, i), start, i));
            }else if(c == '+'){
                tokens.add(new Token(Token.TokenType.OP_ADD, new char[]{c}, i, i++));
            }else if(c == '-'){
                tokens.add(new Token(Token.TokenType.OP_SUB, new char[]{c}, i, i++));
            }else if(c == '*'){
                if(code[i + 1] == '*')
                    tokens.add(new Token(Token.TokenType.OP_POW, new char[]{c,c}, i, i+=2));
                else tokens.add(new Token(Token.TokenType.OP_MUL, new char[]{c}, i, i++));
            }else if(c == '<'){
                if(code[i + 1] == '>')
                    tokens.add(new Token(Token.TokenType.OP_NEQ, new char[]{c,'>'}, i, i+=2));
                else if(code[i + 1] == '=')
                    tokens.add(new Token(Token.TokenType.OP_LE, new char[]{c,'='}, i, i+=2));
                else if(code[i + 1] == '-')
                    tokens.add(new Token(Token.TokenType.ASSIGN, new char[]{c,'-'}, i, i+=2));
                else tokens.add(new Token(Token.TokenType.OP_LT, new char[]{c}, i, i++));
            }else if(c == '>'){
                if(code[i + 1] == '=')
                    tokens.add(new Token(Token.TokenType.OP_GE, new char[]{c,'='}, i, i+=2));
                else tokens.add(new Token(Token.TokenType.OP_GT, new char[]{c}, i, i++));
            }else if(c == '/'){
                tokens.add(new Token(Token.TokenType.OP_DIV, new char[]{c}, i, i++));
            }else if(c == '='){
                tokens.add(new Token(Token.TokenType.OP_EQ, new char[]{c}, i, i++));
            }else if(c == ';'){
                tokens.add(new Token(Token.TokenType.SEMICOLON, new char[]{c}, i, i++));
            }else if(c == '.'){
                tokens.add(new Token(Token.TokenType.DOT, new char[]{c}, i, i++));
            }else if(c == '\'' || c == '"'){
                StringBuilder str = new StringBuilder();
                int start = i++;
                while(i < code.length && code[i] != c){
                    str.append(code[i++]);
                }
                tokens.add(new Token(Token.TokenType.STRING, str.toString().toCharArray(), start, i++));
            }else if(c == '{'){
                tokens.add(new Token(Token.TokenType.LCURLY, new char[]{c}, i, i++));
            }else if(c == '}'){
                tokens.add(new Token(Token.TokenType.RCURLY, new char[]{c}, i, i++));
            }else if(c == '('){
                tokens.add(new Token(Token.TokenType.LPAREN, new char[]{c}, i, i++));
            }else if(c == ')'){
                tokens.add(new Token(Token.TokenType.RPAREN, new char[]{c}, i, i++));
            }else if(c == '['){
                tokens.add(new Token(Token.TokenType.LBRACKET, new char[]{c}, i, i++));
            }else if(c == ']'){
                tokens.add(new Token(Token.TokenType.RBRACKET, new char[]{c}, i, i++));
            }else if(Character.isLetter(c)){ //keyword
                int start = i;
                while(i < code.length && Character.isLetter(code[i])) i++;
                String str = new String(Arrays.copyOfRange(code, start, i));
                Token.TokenType tokenType = switch(str.toLowerCase()){
                    case "in" -> Token.TokenType.OP_IN;
                    case "between" -> Token.TokenType.OP_BETWEEN;
                    case "and" -> Token.TokenType.OP_AND;
                    case "or" -> Token.TokenType.OP_OR;
                    case "database" -> Token.TokenType.DATABASE;
                    case  "delete" -> Token.TokenType.DELETE;
                    case  "fetch" -> Token.TokenType.FETCH;
                    case  "to" -> Token.TokenType.TO;
                    case  "from" -> Token.TokenType.FROM;
                    case  "connect" -> Token.TokenType.CONNECT;
                    case  "as" -> Token.TokenType.AS;
                    case  "csv" -> Token.TokenType.CSV;
                    case  "psv" -> Token.TokenType.PSV;
                    case  "json" -> Token.TokenType.JSON;
                    case  "with" -> Token.TokenType.WITH;
                    case  "using" -> Token.TokenType.USING;
                    case  "get" -> Token.TokenType.GET;
                    case  "export" -> Token.TokenType.EXPORT;
                    case  "download" -> Token.TokenType.DOWNLOAD;
                    case  "over" -> Token.TokenType.OVER;
                    case  "when" -> Token.TokenType.WHEN;
                    case  "else" -> Token.TokenType.ELSE;
                    case  "branch" -> Token.TokenType.BRANCH;
                    case  "pipeline" -> Token.TokenType.PIPELINE;
                    case  "select" -> Token.TokenType.SELECT;
                    case  "where" -> Token.TokenType.WHERE;
                    case  "order" -> Token.TokenType.ORDER;
                    case  "by" -> Token.TokenType.BY;
                    case  "limit" -> Token.TokenType.LIMIT;
                    case  "group" -> Token.TokenType.GROUP;
                    case  "having" -> Token.TokenType.HAVING;
                    case  "inner" -> Token.TokenType.INNER;
                    case  "outer" -> Token.TokenType.OUTER;
                    case  "left" -> Token.TokenType.LEFT;
                    case  "right" -> Token.TokenType.RIGHT;
                    case  "natural" -> Token.TokenType.NATURAL;
                    case  "join" -> Token.TokenType.JOIN;
                    case  "null" -> Token.TokenType.NULL;
                    case  "sort" -> Token.TokenType.SORT;
                    case  "fill" -> Token.TokenType.FILL;
                    case  "map" -> Token.TokenType.MAP;
                    case  "distinct" -> Token.TokenType.DISTINCT;
                    case  "skip" -> Token.TokenType.SKIP;
                    case  "count" -> Token.TokenType.COUNT;
                    case  "sum" -> Token.TokenType.SUM;
                    case  "avg" -> Token.TokenType.AVG;
                    case  "median" -> Token.TokenType.MEDIAN;
                    case  "mode" -> Token.TokenType.MODE;
                    case  "min" -> Token.TokenType.MIN;
                    case  "max" -> Token.TokenType.MAX;
                    case  "range" -> Token.TokenType.RANGE;
                    case  "std" -> Token.TokenType.STD;
                    case  "partition" -> Token.TokenType.PARTITION;
                    case  "window" -> Token.TokenType.WINDOW;
                    case  "quantile" -> Token.TokenType.QUANTILE;
                    case  "decile" -> Token.TokenType.DECILE;
                    case  "percentile" -> Token.TokenType.PERCENTILE;
                    case  "cumsum" -> Token.TokenType.CUMSUM;
                    case  "cumprod" -> Token.TokenType.CUMPROD;
                    case  "cummax" -> Token.TokenType.CUMMAX;
                    case  "cummin" -> Token.TokenType.CUMMIN;
                    case  "cumcount" -> Token.TokenType.CUMCOUNT;
                    case  "true" -> Token.TokenType.TRUE;
                    case  "false" -> Token.TokenType.FALSE;
                    case  "apply" -> Token.TokenType.APPLY;
                    default -> null;
                };
                if(tokenType == null){
                    //TODO: handle unknown token
                    return new Token[]{newEofToken(i, UNKNOWN_TOKEN)};
                }
                tokens.add(new Token(tokenType, str.toCharArray(), start, i));
            }else {
                return new Token[]{newEofToken(i, UNKNOWN_TOKEN)};
            }
        }

        tokens.add(newEofToken(i, ANALYZE_SUCCESS));

        return tokens.toArray(new Token[0]);
    }

    private static Token newEofToken(int i, int code){
        return new Token(Token.TokenType.EOF, null, i, code);
    }

}
