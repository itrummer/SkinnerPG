package expressions.normalization;

import java.util.HashSet;
import java.util.Set;

import net.sf.jsqlparser.schema.Column;
import query.ColumnRef;

/**
 * Collects all tables and columns referenced in a given expression.
 * 
 * @author immanueltrummer
 *
 */
public class CollectReferencesVisitor extends PlainVisitor {
	/**
	 * Contains the set of table aliases that the expression references.
	 */
	public final Set<String> mentionedTables = new HashSet<String>();
	/**
	 * Contains the set of columns (with table name) the the
	 * expression refers to.
	 */
	public final Set<ColumnRef> mentionedColumns = new HashSet<ColumnRef>();
	@Override
	public void visit(Column tableColumn) {
		String tableName = tableColumn.getTable().getName();
		String columnName = tableColumn.getColumnName();
		mentionedTables.add(tableName);
		mentionedColumns.add(new ColumnRef(tableName, columnName));
	}
}
