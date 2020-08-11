package expressions.normalization;

import java.util.Map;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/**
 * Normalizes column references in the visited expression by
 * - transforming all references to lower case letters,
 * - and making implicit table references explicit.
 * 
 * @author immanueltrummer
 *
 */
public class NormalizeColumnsVisitor extends CopyVisitor {
	/**
	 * Maps column names to associated table aliases.
	 */
	public final Map<String, String> columnToAlias;
	/**
	 * Initializes visitor with information that allows to
	 * infer implicit table references.
	 * 
	 * @param columnToAlias	maps columns to associated table aliases
	 */
	public NormalizeColumnsVisitor(Map<String, String> columnToAlias) {
		this.columnToAlias = columnToAlias;
	}
	@Override
	public void visit(Column arg0) {
		Column newColumn = new Column();
		String columnName = arg0.getColumnName().toLowerCase();
		String tableName = arg0.getTable()!=null?
				arg0.getTable().getName().toLowerCase():
					columnToAlias.get(columnName);
		newColumn.setColumnName(columnName);
		newColumn.setTable(new Table(tableName));
		exprStack.push(newColumn);
	}

}
