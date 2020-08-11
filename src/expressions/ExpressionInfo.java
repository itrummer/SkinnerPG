package expressions;

import java.util.Set;

import expressions.normalization.CollectReferencesVisitor;
import expressions.normalization.NormalizeColumnsVisitor;
import expressions.normalization.SubstitutionVisitor;
import net.sf.jsqlparser.expression.Expression;
import query.ColumnRef;
import query.QueryInfo;

/**
 * Stores information on one expression (e.g., a predicate).
 * 
 * @author immanueltrummer
 *
 */
public class ExpressionInfo {
	/**
	 * Contains information about the query surrounding the expression.
	 */
	public final QueryInfo queryInfo;
	/**
	 * The original expression.
	 */
	public final Expression originalExpression;
	/**
	 * Expression after substituting place-holders by expressions.
	 */
	public final Expression afterSubstitution;
	/**
	 * Expression after normalizing column references.
	 */
	public final Expression finalExpression;
	/**
	 * Alias assigned to expression in original query (if any).
	 */
	public final String alias;
	/**
	 * Aliases of all tables mentioned in the expression.
	 */
	public final Set<String> aliasesMentioned;
	/**
	 * All columns mentioned in the expression.
	 */
	public final Set<ColumnRef> columnsMentioned;
	/**
	 * Initializes the expression info.
	 * 
	 * @param queryInfo		meta-data about input query
	 * @param expression	the expression
	 * @param alias			expression alias (may be null)
	 */
	public ExpressionInfo(QueryInfo queryInfo, 
			Expression expression, String alias) {
		//System.out.println("Original: " + expression);
		// Store input parameters
		this.queryInfo = queryInfo;
		this.originalExpression = expression;
		this.alias = alias;
		// Substitute references to expressions in SELECT clause
		SubstitutionVisitor substitutionVisitor = 
				new SubstitutionVisitor(queryInfo.aliasToExpression);
		originalExpression.accept(substitutionVisitor);
		this.afterSubstitution = substitutionVisitor.exprStack.pop();
		//System.out.println("Substituted: " + afterSubstitution);
		// Complete column references by inferring table aliases
		NormalizeColumnsVisitor normalizationVisitor =
				new NormalizeColumnsVisitor(queryInfo.columnToAlias);
		afterSubstitution.accept(normalizationVisitor);
		this.finalExpression = normalizationVisitor.exprStack.pop();
		//System.out.println("Normalized: " + finalExpression);
		// Collect references in the given expression
		CollectReferencesVisitor collectorVisitor =
				new CollectReferencesVisitor();
		finalExpression.accept(collectorVisitor);
		this.aliasesMentioned = collectorVisitor.mentionedTables;
		this.columnsMentioned = collectorVisitor.mentionedColumns;
	}
	@Override
	public String toString() {
		return finalExpression.toString();
	}
}
