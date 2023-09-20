package org.apache.seatunnel.transform.sql.zeta.visit;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.values.ValuesStatement;

import java.util.List;

public class ZetaSelectVisitor extends SelectVisitorAdapter {

    private final ZetaStatementVisitor zetaStatementVisitor;

    public ZetaSelectVisitor(ZetaStatementVisitor zetaStatementVisitor) {
        this.zetaStatementVisitor = zetaStatementVisitor;
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        if (plainSelect.getLimit() != null || plainSelect.getOffset() != null) {
            throw new IllegalArgumentException("Unsupported limit,offset syntax");
        }
        if (plainSelect.getJoins() != null) {
            throw new IllegalArgumentException("Unsupported table join syntax");
        }
        if (plainSelect.getOrderByElements() != null) {
            throw new IllegalArgumentException("Unsupported order by syntax");
        }
        if (plainSelect.getGroupBy() != null) {
            throw new IllegalArgumentException("Unsupported group by syntax");
        }
        if (plainSelect.getDistinct() != null) {
            throw new IllegalArgumentException("Unsupported distinct syntax");
        }
        if (plainSelect.getHaving() != null) {
            throw new IllegalArgumentException("Unsupported having syntax");
        }

        // from
        FromItem fromItem = plainSelect.getFromItem();
        if (null != fromItem) {
            ZetaFromItemVisitor fromItemVisitor = new ZetaFromItemVisitor(zetaStatementVisitor);
            fromItem.accept(fromItemVisitor);
        } else {
            throw new IllegalArgumentException("Unsupported not form syntax");
        }

        // where
        ZetaExpressionVisitor zetaExpressionVisitor =
                new ZetaExpressionVisitor(zetaStatementVisitor);
        Expression where = plainSelect.getWhere();

        // type mapping not need where
        boolean needWhere = zetaStatementVisitor.isNeedWhere();
        if (!needWhere) where = null;
        if (where != null) {
            where.accept(zetaExpressionVisitor);
            // clean
            zetaStatementVisitor.getOutputFieldNames().clean();
            zetaStatementVisitor.getOutputDataTypes().clean();
            zetaStatementVisitor.getOutputFields().clean();

            if (!zetaExpressionVisitor.isKeep()) return;
        }

        // select
        ZetaSelectItemVisitor zetaSelectItemVisitor =
                new ZetaSelectItemVisitor(zetaStatementVisitor, zetaExpressionVisitor);
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        for (SelectItem selectItem : selectItems) {
            selectItem.accept(zetaSelectItemVisitor);
        }
    }

    /** INTERSECT、EXCEPT、MINUS、UNION */
    @Override
    public void visit(SetOperationList setOpList) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** with as t1(select 1 as id) select id from t1 */
    @Override
    public void visit(WithItem withItem) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }

    /** select id,name from values(1,'zs') as (id,name) */
    @Override
    public void visit(ValuesStatement aThis) {
        throw new IllegalArgumentException("Only supported DQL(select) SQL");
    }
}
