package hello1.koddata.kodlang.ast;

public class DataFrameDeclaration extends Expression {

    final Expression dataframe;
    final Expression rename;

    public DataFrameDeclaration(Expression dataframe, Expression rename){
        super(StatementType.DATAFRAME);
        this.dataframe = dataframe;
        this.rename = rename;
    }

}
