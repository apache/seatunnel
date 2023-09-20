package org.apache.seatunnel.transform.sql.zeta.visit;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.ParenthesisFromItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.ValuesList;

public class ZetaFromItemVisitor extends FromItemVisitorAdapter {
    ZetaStatementVisitor zetaStatementVisitor;

    public ZetaFromItemVisitor(ZetaStatementVisitor zetaStatementVisitor) {
        this.zetaStatementVisitor = zetaStatementVisitor;
    }

    @Override
    public void visit(Table table) {
        if (table.getSchemaName() != null) {
            throw new IllegalArgumentException("Unsupported from schema syntax");
        }
        if (table.getAlias() != null) {
            throw new IllegalArgumentException("Unsupported from table alias syntax");
        }
        String inputTableName = zetaStatementVisitor.getInputTableName();
        String catalogTableName = zetaStatementVisitor.getCatalogTableName();
        String tableName = table.getName();
        if (!inputTableName.equalsIgnoreCase(tableName)
                && !catalogTableName.equalsIgnoreCase(tableName)) {
            throw new IllegalArgumentException(
                    String.format("Table name: %s not found", tableName));
        }

        System.out.printf("from table name: %s%n", tableName);
    }

    @Override
    public void visit(SubSelect subSelect) {
        throw new IllegalArgumentException("Unsupported from sub table syntax");
    }

    @Override
    public void visit(SubJoin subjoin) {
        throw new IllegalArgumentException("Unsupported from sub table syntax");
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect) {
        throw new IllegalArgumentException("Unsupported from sub table syntax");
    }

    @Override
    public void visit(ValuesList valuesList) {
        throw new IllegalArgumentException("Unsupported from value list syntax");
    }

    @Override
    public void visit(TableFunction valuesList) {
        throw new IllegalArgumentException("Unsupported from table function syntax");
    }

    @Override
    public void visit(ParenthesisFromItem aThis) {
        throw new IllegalArgumentException("Unsupported from parenthesis syntax");
    }
}
