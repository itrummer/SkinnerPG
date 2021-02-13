package postprocessing;

import java.util.List;

import config.JoinConfig;
import config.LogConfig;
import config.MasterConfig;
import config.NamingConfig;
import connector.PgConnector;
import expressions.ExpressionInfo;
import expressions.normalization.PostProcessingVisitor;
import joining.JoinSummary;
import query.QueryInfo;

/**
 * Performs post-processing on the result of the join phase,
 * including grouping, aggregation, and sorting.
 * 
 * @author immanueltrummer
 *
 */
public class PostProcessor {
	/**
	 * Performs post-processing and stores result in dedicated table.
	 * 
	 * @param query			information on input query
	 * @param joinSummary	summary of join phase
	 * @param queryID		ID of query
	 * @return				summary of post-processing steps
	 * @throws Exception
	 */
	public static PostSummary process(QueryInfo query, JoinSummary joinSummary, 
			String queryID) throws Exception {
		System.out.println("Starting post-processing ...");
		// Drop table if it exists
		String finalTable = NamingConfig.FINAL_TBL + queryID;
		PgConnector.dropTable(finalTable);
		// Create query for post-processing
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("CREATE TABLE ");
		sqlBuilder.append(finalTable);
		sqlBuilder.append(" AS (SELECT ");
		sqlBuilder.append(substitutedExprList(
				query.selectExpressions, true));
		sqlBuilder.append(" FROM ");
		// De-duplicate join result if necessary
		if (JoinConfig.deleteProcessed) {
			sqlBuilder.append(joinSummary.resultTable);	
		} else {
			sqlBuilder.append(" (SELECT DISTINCT * FROM ");
			sqlBuilder.append(joinSummary.resultTable);
			sqlBuilder.append(") ");
		}
		sqlBuilder.append(" AS joinResultTuples");
		if (!query.groupByExpressions.isEmpty()) {
			sqlBuilder.append("GROUP BY ");
			sqlBuilder.append(substitutedExprList(
					query.groupByExpressions, false));
			sqlBuilder.append(" ");
		}
		if (query.havingExpression != null) {
			sqlBuilder.append("HAVING ");
			sqlBuilder.append(substituteColumns(query.havingExpression));
			sqlBuilder.append(" ");
		}
		if (!query.orderByExpressions.isEmpty()) {
			sqlBuilder.append("ORDER BY ");
			sqlBuilder.append(substitutedExprList(
					query.orderByExpressions, false));
		}
		sqlBuilder.append(");");
		boolean noTimeout = PgConnector.updateOrTimeout(
				sqlBuilder.toString(), 
				MasterConfig.perPhaseTimeout);
		// Create empty final result table if required
		if (!noTimeout) {
			PgConnector.update("create table " + 
					finalTable + " as (select 'timeout');");
		}
		// Report timeout if any
		if (LogConfig.VERBOSE) {
			System.out.println("Post-processing success: " + 
					noTimeout);
		}
		// Return post-processing summary
		return new PostSummary(finalTable, !noTimeout);
	}
	/**
	 * Given a list of expressions, replace references to original table columns
	 * in each expression and concatenate them, separated by commas.
	 * 
	 * @param expressions	list of expressions to transform and concatenate
	 * @param useAliases	whether to include expression aliases
	 * @return				string concatenating expressions after substitution
	 */
	static String substitutedExprList(
			List<ExpressionInfo> expressions, boolean useAliases) {
		StringBuilder sqlBuilder = new StringBuilder();
		boolean firstItem = true;
		for (ExpressionInfo exprInfo : expressions) {
			if (!firstItem) {
				sqlBuilder.append(", ");
			}
			sqlBuilder.append(substituteColumns(exprInfo));
			if (useAliases && exprInfo.alias != null) {
				sqlBuilder.append(" AS ");
				sqlBuilder.append(exprInfo.alias);
			}
			firstItem = false;
		}
		return sqlBuilder.toString();
	}
	/**
	 * Given an expression, substitutes references to columns in
	 * the original tables by references to corresponding columns
	 * in the table holding the join result.
	 * 
	 * @param exprInfo	expression to transform and associated meta-data
	 * @return			expression after substitutions
	 */
	static String substituteColumns(ExpressionInfo exprInfo) {
		PostProcessingVisitor postVisit = 
				new PostProcessingVisitor();
		exprInfo.finalExpression.accept(postVisit);
		return postVisit.exprStack.pop().toString();
	}
}
