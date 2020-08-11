package expressions.normalization;

import net.sf.jsqlparser.schema.Column;

/**
 * This visitor transform an expression for use in the final
 * SQL query doing post-processing: it replaces references
 * to table columns of the form Alias.Column by a reference
 * of the form Alias_Column (which is its name in the final
 * join result table). 
 * 
 * @author immanueltrummer
 *
 */
public class PostProcessingVisitor extends CopyVisitor {
	@Override
	public void visit(Column arg0) {
		String alias = arg0.getTable().getName();
		String column = arg0.getColumnName();
		Column newColumn = new Column(alias + "_" + column);
		exprStack.push(newColumn);
	}

}
