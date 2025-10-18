package hello1.koddata.kodlang.ast;

public class DownloadStatement extends Statement {

    final Expression src;

    public DownloadStatement(Expression src){
        super(StatementType.DOWNLOAD);
        this.src = src;
    }

}
