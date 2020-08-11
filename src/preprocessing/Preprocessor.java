package preprocessing;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import catalog.ColumnInfo;
import config.JoinConfig;
import config.NamingConfig;
import config.PreConfig;
import config.PreCopyMode;
import connector.PgConnector;
import expressions.ExpressionInfo;
import query.ColumnRef;
import query.QueryInfo;

/**
 * Filters query tables via unary predicates and stores
 * result in newly created tables. Creates hash tables
 * for columns with binary equality join predicates.
 * 
 * @author immanueltrummer
 *
 */
public class Preprocessor {
	/**
	 * Executes pre-processing via Postgres. 
	 * 
	 * @param query		the query to pre-process
	 * @return 			summary of pre-processing steps
	 */
	public static PreSummary process(QueryInfo query) throws Exception {
		// Disable previous timeouts if any
		PgConnector.setNoTimeout();
		// Initialize pre-processing summary
		PreSummary preSummary = new PreSummary();
		// Collect columns required for joins and post-processing
		Set<ColumnRef> requiredCols = new HashSet<ColumnRef>();
		requiredCols.addAll(query.colsForJoins);
		requiredCols.addAll(query.colsForPostProcessing);
		System.out.println("Required columns: " + requiredCols);
		// Iterate over query aliases
		for (String alias : query.aliasToTable.keySet()) {
			// Collect required columns (for joins and post-processing) for this table
			List<ColumnRef> curRequiredCols = new ArrayList<ColumnRef>();
			for (ColumnRef requiredCol : requiredCols) {
				if (requiredCol.aliasName.equals(alias)) {
					curRequiredCols.add(requiredCol);
				}
			}
			// Create comma-separated list of relevant columns
			// for current table - once with types (for SQL
			// create statements) and once without types
			// (for SQL select clauses).
			createColumnLists(query, alias, curRequiredCols, preSummary);
			// Get applicable unary predicates
			ExpressionInfo curUnaryPred = null;
			for (ExpressionInfo exprInfo : query.unaryPredicates) {
				if (exprInfo.aliasesMentioned.contains(alias)) {
					curUnaryPred = exprInfo;
				}
			}
			// Depending on pre-processing mode, 
			// put unary predicate on todo list.
			if (PreConfig.preCopyMode == PreCopyMode.COPY_NONE) {
				preSummary.aliasToUnaryTodo.put(alias, curUnaryPred);
			}
			// Filter and project if enabled
			if (PreConfig.preCopyMode == PreCopyMode.COPY_ALL ||
					(PreConfig.preCopyMode == PreCopyMode.COPY_CONSTRAINED &&
					curUnaryPred != null)) {
				filterProject(query, alias, curUnaryPred,
						curRequiredCols, preSummary);
			} else {
				String baseTable = query.aliasToTable.get(alias);
				String clusteredTable = NamingConfig.CLUSTER_TBL + baseTable;
				preSummary.aliasToTable.put(alias, clusteredTable);
			}
		}
		// Create missing indices for columns containing batch IDs
		// and for columns involved in equi-joins.
		System.out.println("Creating indices ...");
		createIndices(query, preSummary);
		return preSummary;
	}
	/**
	 * Create comma-separated list of relevant columns for current table - 
	 * once with types (for SQL create statements) and once without types
	 * (for SQL select clauses). 
	 * 
	 * @param query				the query to process
	 * @param alias				concatenate columns of table behind this alias
	 * @param requiredCols		required columns for joins and post-processing		
	 * @param preSummary		store columns lists in here
	 */
	static void createColumnLists(QueryInfo query, String alias,
			List<ColumnRef> requiredCols, PreSummary preSummary) {
		StringBuilder columnBuilder = new StringBuilder();
		StringBuilder typedColumnBuilder = new StringBuilder();
		boolean firstColumn = true;
		for (ColumnRef colRef : requiredCols) {
			if (!firstColumn) {
				columnBuilder.append(", ");
				typedColumnBuilder.append(", ");
			}
			columnBuilder.append(colRef.columnName);
			typedColumnBuilder.append(colRef.columnName);
			typedColumnBuilder.append(" ");
			ColumnInfo colInfo = query.colRefToInfo.get(colRef);
			typedColumnBuilder.append(colInfo.columnType);
			firstColumn = false;
		}
		preSummary.aliasToNonIDcols.put(
				alias, columnBuilder.toString());
		preSummary.aliasToTypedNonIDcols.put(
				alias, typedColumnBuilder.toString());
	}
	/**
	 * Creates a new temporary table containing remaining tuples
	 * after applying unary predicates, project on columns that
	 * are required for following steps.
	 * 
	 * @param query			query to pre-process
	 * @param alias			alias of table to filter
	 * @param unaryPred		unary predicate on that table
	 * @param requiredCols	project on those columns
	 * @param preSummary	summary of pre-processing steps
	 */
	static void filterProject(QueryInfo query, String alias, ExpressionInfo unaryPred, 
			List<ColumnRef> requiredCols, PreSummary preSummary) throws Exception {
		System.out.println("Filtering, projection, and clustering for " + alias + " ...");
		// Name of table containing temporary result
		String filteredAlias = NamingConfig.FILTER_TBL + alias;
		// Clean up from prior runs if necessary
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("DROP TABLE IF EXISTS ");
		sqlBuilder.append(filteredAlias);
		sqlBuilder.append(";");
		PgConnector.update(sqlBuilder.toString());
		// Build query creating temporary table
		sqlBuilder = new StringBuilder();
		sqlBuilder.append("CREATE TEMP TABLE ");
		sqlBuilder.append(filteredAlias);
		sqlBuilder.append(" AS ");
		/*
		sqlBuilder.append(" (");
		String createCols = preSummary.aliasToTypedNonIDcols.get(alias);
		sqlBuilder.append(createCols);
		if (!createCols.isEmpty()) {
			sqlBuilder.append(", ");
		}
		sqlBuilder.append("SkinnerBatchID integer);");
		PgConnector.update(sqlBuilder.toString());
		// Fill temporary table
		sqlBuilder = new StringBuilder();
		sqlBuilder.append("INSERT INTO ");
		sqlBuilder.append(filteredAlias);
		*/
		sqlBuilder.append(" (SELECT ");
		String selectCols = preSummary.aliasToNonIDcols.get(alias);
		sqlBuilder.append(selectCols);
		if (!selectCols.isEmpty()) {
			sqlBuilder.append(", ");
		}
		String table = query.aliasToTable.get(alias);
		sqlBuilder.append("((id * 19 + 23) % ");
		sqlBuilder.append(JoinConfig.nrBatches);
		sqlBuilder.append(")::smallint AS ");
		sqlBuilder.append(NamingConfig.BATCH_ID_COLUMN);
		sqlBuilder.append(" FROM ");
		sqlBuilder.append(table);
		//sqlBuilder.append(" SkinnerBatchID FROM ");
		//sqlBuilder.append(table + "batched");
		sqlBuilder.append(" AS ");
		sqlBuilder.append(alias);
		if (unaryPred != null) {
			sqlBuilder.append(" WHERE ");
			sqlBuilder.append(unaryPred.originalExpression.toString());			
		}
		sqlBuilder.append(" ORDER BY ");
		sqlBuilder.append(NamingConfig.BATCH_ID_COLUMN);
		sqlBuilder.append(");");
		//PgConnector.explain(" ANALYZE " + sqlBuilder.toString());
		PgConnector.update(sqlBuilder.toString());
		// Collect remaining batches
		if (unaryPred != null) {
			sqlBuilder = new StringBuilder();
			sqlBuilder.append("SELECT DISTINCT ");
			sqlBuilder.append(NamingConfig.BATCH_ID_COLUMN);
			sqlBuilder.append(" FROM ");
			sqlBuilder.append(filteredAlias);
			ResultSet result = PgConnector.query(sqlBuilder.toString());
			// Collect qualifying batch IDs
			List<Integer> todoBatches = new ArrayList<Integer>();
			while (result.next()) {
				todoBatches.add(result.getInt(1));
			}
			preSummary.aliasToTodoBatches.put(alias, todoBatches);
		}
		// 
		/*
		sqlBuilder.append(filteredAlias);
		sqlBuilder.append(" AS (SELECT ");
		// Concatenate columns for SELECT clause
		StringBuilder columnBuilder = new StringBuilder();
		boolean firstColumn = true;
		for (ColumnRef colRef : requiredCols) {
			if (!firstColumn) {
				columnBuilder.append(", ");
			}
			columnBuilder.append(colRef.columnName);
			firstColumn = false;
		}
		String columnsList = columnBuilder.toString();
		sqlBuilder.append(columnsList);
		preSummary.aliasToNonIDcols.put(alias, columnsList);
		if (!firstColumn) {
			sqlBuilder.append(", ");
		}
		sqlBuilder.append(", (ctid::text::point)[1]::int % ");
		sqlBuilder.append(" FROM ");
		String table = query.aliasToTable.get(alias);
		sqlBuilder.append(table);
		sqlBuilder.append(" AS ");
		sqlBuilder.append(alias);
		if (unaryPred != null) {
			sqlBuilder.append(" WHERE ");
			sqlBuilder.append(unaryPred.originalExpression.toString());			
		}
		sqlBuilder.append(");");
		PgConnector.update(sqlBuilder.toString());
		*/
		// Update pre-processing summary
		preSummary.tempTables.add(filteredAlias);
		preSummary.aliasToTable.put(alias, filteredAlias);
	}
	/**
	 * Create indices on equality join columns if not yet available.
	 * 
	 * @param query			query for which to create indices
	 * @param preSummary	summary of pre-processing steps executed so far
	 * @throws Exception
	 */
	static void createIndices(QueryInfo query, PreSummary preSummary) throws Exception {
		// Iterate over columns in equi-joins
		for (ColumnRef colRef : query.equiJoinCols) {
			StringBuilder sqlBuilder = new StringBuilder();
			sqlBuilder.append("CREATE INDEX IF NOT EXISTS ");
			String alias = colRef.aliasName;
			String table = preSummary.aliasToTable.get(alias);
			/*
			sqlBuilder.append(table);
			sqlBuilder.append("_");
			sqlBuilder.append(colRef.columnName);
			*/
			sqlBuilder.append(NamingConfig.indexName(table, colRef.columnName));
			sqlBuilder.append(" ON ");
			sqlBuilder.append(table);
			sqlBuilder.append(" (");
			sqlBuilder.append(colRef.columnName);
			sqlBuilder.append(");");
			System.out.println("Creating index on " + table + " ...");
			PgConnector.update(sqlBuilder.toString());
		}
		// Create indices for batch numbers (for fast
		// retrieval and deletion of tuples in specific
		// batches).
		if (JoinConfig.batchIDindexThreshold >= 1.0) {
			for (String alias : preSummary.aliasToTable.keySet()) {
				String table = preSummary.aliasToTable.get(alias);
				StringBuilder sqlBuilder = new StringBuilder();
				// Create index
				sqlBuilder.append("CREATE INDEX IF NOT EXISTS ");
				/*
				String indexName = alias + "_batchNrIndex";
				sqlBuilder.append(indexName);
				*/
				sqlBuilder.append(NamingConfig.indexName(table, NamingConfig.BATCH_ID_COLUMN));
				sqlBuilder.append(" ON ");
				sqlBuilder.append(table);
				sqlBuilder.append(" (");
				sqlBuilder.append(NamingConfig.BATCH_ID_COLUMN);
				sqlBuilder.append(");");
				System.out.println("Creating index on " + table + " ...");
				PgConnector.update(sqlBuilder.toString());
				// Make index clustered
				/*
				sqlBuilder = new StringBuilder();
				sqlBuilder.append("CLUSTER ");
				sqlBuilder.append(table);
				sqlBuilder.append(" USING ");
				sqlBuilder.append(indexName);
				sqlBuilder.append(";");
				PgConnector.update(sqlBuilder.toString());
				*/
			}			
		}
	}
}
