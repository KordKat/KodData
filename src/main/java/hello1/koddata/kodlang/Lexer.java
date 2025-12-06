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
            }else if(c == ','){
                tokens.add(new Token(Token.TokenType.COMMA, new char[]{','}, i, i++));
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
                    case "and" -> Token.TokenType.OP_AND;
                    case "or" -> Token.TokenType.OP_OR;
                    case  "pipeline" -> Token.TokenType.PIPELINE;
                    case  "null" -> Token.TokenType.NULL;
                    case  "true" -> Token.TokenType.TRUE;
                    case  "false" -> Token.TokenType.FALSE;
                    default -> null;
                };
                if(tokenType == null) tokens.add(new Token(Token.TokenType.NIDENTIFIER, str.toCharArray(), start, i));
                else tokens.add(new Token(tokenType, str.toCharArray(), start, i));
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
