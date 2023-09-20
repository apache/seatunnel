package org.apache.seatunnel.transform.sql.zeta.visit;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.sql.zeta.agg.OutputDataTypes;
import org.apache.seatunnel.transform.sql.zeta.agg.OutputFieldNames;
import org.apache.seatunnel.transform.sql.zeta.agg.OutputFields;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;

public class ZetaSelectItemVisitor extends SelectItemVisitorAdapter {
    private final ZetaStatementVisitor zetaStatementVisitor;
    private final ZetaExpressionVisitor zetaExpressionVisitor;

    public ZetaSelectItemVisitor(
            ZetaStatementVisitor zetaStatementVisitor,
            ZetaExpressionVisitor zetaExpressionVisitor) {
        this.zetaStatementVisitor = zetaStatementVisitor;
        this.zetaExpressionVisitor = zetaExpressionVisitor;
    }

    /** select * */
    @Override
    public void visit(AllColumns columns) {
        SeaTunnelRowType inputRowType = zetaStatementVisitor.getInputRowType();
        OutputFieldNames outputFieldNames = zetaStatementVisitor.getOutputFieldNames();
        outputFieldNames.addAll(inputRowType.getFieldNames());

        OutputDataTypes outputDataTypes = zetaStatementVisitor.getOutputDataTypes();
        outputDataTypes.addAll(inputRowType.getFieldTypes());

        OutputFields outputFields = zetaStatementVisitor.getOutputFields();
        outputFields.addAll(zetaStatementVisitor.getInputFields());
    }

    /** select t1.* */
    @Override
    public void visit(AllTableColumns columns) {
        throw new IllegalArgumentException("Unsupported table alias name syntax");
    }

    /** select id */
    @Override
    public void visit(SelectExpressionItem item) {
        OutputFieldNames outputFieldNames = zetaStatementVisitor.getOutputFieldNames();
        Expression expression = item.getExpression();
        Alias alias = item.getAlias();
        if (alias != null) {
            outputFieldNames.add(alias.getName());
        } else {
            if (expression instanceof Column) {
                String columnName = ((Column) expression).getColumnName();
                outputFieldNames.add(columnName);
            } else {
                outputFieldNames.add(expression.toString());
            }
        }
        expression.accept(zetaExpressionVisitor);
    }
}
