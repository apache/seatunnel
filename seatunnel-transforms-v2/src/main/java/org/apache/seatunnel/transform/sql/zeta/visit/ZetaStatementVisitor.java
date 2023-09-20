package org.apache.seatunnel.transform.sql.zeta.visit;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;
import org.apache.seatunnel.transform.sql.zeta.agg.OutputDataTypes;
import org.apache.seatunnel.transform.sql.zeta.agg.OutputFieldNames;
import org.apache.seatunnel.transform.sql.zeta.agg.OutputFields;

import lombok.Getter;
import lombok.Setter;
import net.sf.jsqlparser.statement.Block;
import net.sf.jsqlparser.statement.Commit;
import net.sf.jsqlparser.statement.CreateFunctionalStatement;
import net.sf.jsqlparser.statement.DeclareStatement;
import net.sf.jsqlparser.statement.DescribeStatement;
import net.sf.jsqlparser.statement.ExplainStatement;
import net.sf.jsqlparser.statement.IfElseStatement;
import net.sf.jsqlparser.statement.PurgeStatement;
import net.sf.jsqlparser.statement.ResetStatement;
import net.sf.jsqlparser.statement.RollbackStatement;
import net.sf.jsqlparser.statement.SavepointStatement;
import net.sf.jsqlparser.statement.SetStatement;
import net.sf.jsqlparser.statement.ShowColumnsStatement;
import net.sf.jsqlparser.statement.ShowStatement;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.UnsupportedStatement;
import net.sf.jsqlparser.statement.UseStatement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterSession;
import net.sf.jsqlparser.statement.alter.AlterSystemStatement;
import net.sf.jsqlparser.statement.alter.RenameTableStatement;
import net.sf.jsqlparser.statement.alter.sequence.AlterSequence;
import net.sf.jsqlparser.statement.analyze.Analyze;
import net.sf.jsqlparser.statement.comment.Comment;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.schema.CreateSchema;
import net.sf.jsqlparser.statement.create.sequence.CreateSequence;
import net.sf.jsqlparser.statement.create.synonym.CreateSynonym;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.AlterView;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.grant.Grant;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.show.ShowTablesStatement;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.upsert.Upsert;
import net.sf.jsqlparser.statement.values.ValuesStatement;

import java.util.List;

public class ZetaStatementVisitor extends StatementVisitorAdapter {
    private final @Getter String inputTableName;
    private final @Getter String catalogTableName;
    private final @Getter SeaTunnelRowType inputRowType;
    private @Getter @Setter Object[] inputFields;
    private final @Getter List<ZetaUDF> udfList;

    private final @Getter OutputFieldNames outputFieldNames = new OutputFieldNames();
    private final @Getter OutputDataTypes outputDataTypes = new OutputDataTypes();
    private final @Getter OutputFields outputFields = new OutputFields();
    private SeaTunnelRowType outputRowType;
    private @Getter @Setter boolean isNeedWhere = false;

    public ZetaStatementVisitor(
            String inputTableName,
            String catalogTableName,
            SeaTunnelRowType inputRowType,
            Object[] inputFields,
            List<ZetaUDF> udfList) {
        int fieldSize = inputRowType.getTotalFields();
        if (inputFields == null) inputFields = new Object[fieldSize];
        this.inputTableName = inputTableName;
        this.catalogTableName = catalogTableName;
        this.inputRowType = inputRowType;
        this.inputFields = inputFields;
        this.udfList = udfList;
    }

    public SeaTunnelRowType getResultRowType() {
        int nameSize = outputFieldNames.getIndex();
        int typeSize = outputDataTypes.getIndex();
        if (nameSize != typeSize) {
            throw new IllegalArgumentException(
                    "The field name size must be equal to data type size");
        }
        String[] fieldNames = outputFieldNames.toArray();
        SeaTunnelDataType<?>[] dataTypes = outputDataTypes.toArray();
        outputRowType = new SeaTunnelRowType(fieldNames, dataTypes);
        return outputRowType;
    }

    public Object[] getResultFields() {
        int typeSize = outputRowType.getTotalFields();
        int fieldSize = outputFields.getIndex() + 1;
        if (typeSize != fieldSize) {
            throw new IllegalArgumentException("The data type size must be equal to field size");
        }
        return outputFields.toArray();
    }

    public void clean() {
        outputFieldNames.clean();
        outputDataTypes.clean();
        outputFields.clean();
        outputRowType = null;
    }

    @Override
    public void visit(Statements stmts) {
        stmts.accept(this);
    }

    @Override
    public void visit(Select select) {
        SelectBody selectBody = select.getSelectBody();
        ZetaSelectVisitor selectVisitor = new ZetaSelectVisitor(this);
        selectBody.accept(selectVisitor);
    }

    /** insert */
    @Override
    public void visit(Insert insert) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** update */
    @Override
    public void visit(Update update) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** delete */
    @Override
    public void visit(Delete delete) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** merge */
    @Override
    public void visit(Merge merge) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** upsert */
    @Override
    public void visit(Upsert upsert) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** alter */
    @Override
    public void visit(Alter alter) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    @Override
    public void visit(AlterView alterView) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    @Override
    public void visit(AlterSequence alterSequence) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    @Override
    public void visit(AlterSession alterSession) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    @Override
    public void visit(AlterSystemStatement alterSystemStatement) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** rename table */
    @Override
    public void visit(RenameTableStatement renameTableStatement) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** create */
    @Override
    public void visit(CreateIndex createIndex) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    @Override
    public void visit(CreateSchema aThis) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    @Override
    public void visit(CreateTable createTable) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    @Override
    public void visit(CreateView createView) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    @Override
    public void visit(CreateSequence createSequence) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    @Override
    public void visit(CreateFunctionalStatement createFunctionalStatement) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    @Override
    public void visit(CreateSynonym createSynonym) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** drop */
    @Override
    public void visit(Drop drop) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** analyze */
    @Override
    public void visit(Analyze analyze) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** truncate */
    @Override
    public void visit(Truncate truncate) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** execute */
    @Override
    public void visit(Execute execute) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** set */
    @Override
    public void visit(SetStatement set) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** reset */
    @Override
    public void visit(ResetStatement reset) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** show */
    @Override
    public void visit(ShowStatement aThis) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    @Override
    public void visit(ShowColumnsStatement set) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    @Override
    public void visit(ShowTablesStatement showTables) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** use */
    @Override
    public void visit(UseStatement use) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** save point */
    @Override
    public void visit(SavepointStatement savepointStatement) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** roll back */
    @Override
    public void visit(RollbackStatement rollbackStatement) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** commit */
    @Override
    public void visit(Commit commit) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** comment */
    @Override
    public void visit(Comment comment) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** describe */
    @Override
    public void visit(DescribeStatement describe) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** explain */
    @Override
    public void visit(ExplainStatement aThis) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** declare */
    @Override
    public void visit(DeclareStatement aThis) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** grant */
    @Override
    public void visit(Grant grant) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** purge */
    @Override
    public void visit(PurgeStatement purgeStatement) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    @Override
    public void visit(Replace replace) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    @Override
    public void visit(Block block) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    @Override
    public void visit(ValuesStatement values) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    @Override
    public void visit(IfElseStatement ifElseStatement) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    @Override
    public void visit(UnsupportedStatement unsupportedStatement) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }
}
