package joining;

import java.sql.ResultSet;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.postgresql.util.PSQLException;

import catalog.ColumnInfo;
import config.JoinConfig;
import config.LogConfig;
import config.NamingConfig;
import config.PreConfig;
import connector.PgConnector;
import expressions.ExpressionInfo;
import preprocessing.PreSummary;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;

/**
 * Processes a join order with a given timeout on
 * data batches and adds join result to intermediate
 * table if it finishes before timeout. Also keeps
 * track of what data batches have been successfully
 * processed already and indicates termination.
 * 
 * @author immanueltrummer
 *
 */
public class BatchedExecutor {
	/**
	 * Summarizes steps taken during pre-processing.
	 */
	public final PreSummary preSummary;
	/**
	 * SQL string representing select clause.
	 */
	public final String selectSQL;
	/**
	 * SQL string representing join predicates.
	 */
	public final String whereSQL;
	/**
	 * Query for which join orders are executed.
	 */
	public final QueryInfo query;
	/**
	 * Name of table into which join results are inserted.
	 */
	public final String joinResultTable;
	/**
	 * At i-th position: table represented by i-th alias.
	 */
	public final String[] tables;
	/**
	 * At i-th position: name of table holding batches for i-th alias.
	 */
	public final String[] batchTables;
	/**
	 * At i-th position: flag indicating whether the table was
	 * created as a temporary table for query processing or is
	 * a base table in the original database.
	 */
	public final boolean[] isTempTable;
	/**
	 * Scaling factor for progress in each table -
	 * proportional to how much of the remaining work
	 * each tuple batch represents for each table and
	 * updated once batches of a table were processed.
	 */
	public final double[] rewardScaling;
	/**
	 * A list of names for all columns that belong
	 * to final join result table.
	 */
	public final List<String> joinResultColumns = new ArrayList<String>();
	/**
	 * At i-th position: batches of table i that still need processing.
	 */
	public final List<List<Integer>> todoBatches = new ArrayList<List<Integer>>();
	/**
	 * At i-th position: batches of table i that are marked for processing next.
	 */
	public final List<Set<Integer>> currentBatches = new ArrayList<Set<Integer>>();
	/**
	 * At i-th position: number of batches in table i.
	 */
	public final int[] nrBatches;
	/**
	 * At i-th position: number of rows cached for table i.
	 */
	//public final int[] nrCachedRows;
	/**
	 * Flag indicating whether join processing is finished.
	 */
	public boolean finished = false;
	/**
	 * Random generator for batch selection.
	 */
	final Random random = new Random();
	/**
	 * Number of progress updates generated.
	 */
	public long nrProgressUpdates = 0;
	/**
	 * Initializes execution of specific join orders on data batches.
	 * Retrieves some information from database such as cardinality
	 * of base tables after filtering via unary predicates.
	 * 
	 * @param query				query to optimize
	 * @param preSummary		summary of pre-processing
	 * @param joinResultTable	name of table storing join results	
	 * @throws Exception
	 */
	public BatchedExecutor(QueryInfo query, PreSummary preSummary,
			String joinResultTable) throws Exception {
		this.query = query;
		this.preSummary = preSummary;
		this.joinResultTable = joinResultTable;
		//this.batchRatio = batchRatio;
		// SELECT and WHERE clauses remain constant
		int nrJoined = query.nrJoined;
		this.selectSQL = createSelectSQL(query, preSummary);
		this.whereSQL = createWhereSQL(query, preSummary);
		// Extract information about joined tables
		tables = new String[nrJoined];
		batchTables = new String[nrJoined];
		isTempTable = new boolean[nrJoined];
		//cardinalities = new int[nrJoined];
		//batchSizes = new int[nrJoined];
		nrBatches = new int[nrJoined];
		rewardScaling = new double[nrJoined];
		for (int aliasCtr=0; aliasCtr<nrJoined; ++aliasCtr) {
			String alias = query.aliases[aliasCtr];
			// Was initial table replaced during pre-processing?
			boolean curIsTemp = preSummary.aliasToTable.containsKey(alias);
			isTempTable[aliasCtr] = curIsTemp;
			if (curIsTemp) {
				tables[aliasCtr] = preSummary.aliasToTable.get(alias);
			} else {
				tables[aliasCtr] = query.aliasToTable.get(alias);
			}
			// Determine name of table holding tuple batches
			batchTables[aliasCtr] = query.aliases[aliasCtr] + "nextbatch";
			// Extract table cardinality
			//cardinalities[aliasCtr] = PgCatalog.cardinality(tables[aliasCtr]);
			// Calculate batch size
			//batchSizes[aliasCtr] = (int)Math.ceil(cardinalities[aliasCtr] * batchRatio);
			// Calculate number of batches
			//nrBatches[aliasCtr] = (int)Math.ceil((double)cardinalities[aliasCtr]/batchSizes[aliasCtr]);
			//batchSizes[aliasCtr] = (int)Math.ceil((double)cardinalities[aliasCtr] / JoinConfig.nrBatches);
			nrBatches[aliasCtr] = JoinConfig.nrBatches;
			// Initialize todo batches
			if (preSummary.aliasToTodoBatches.containsKey(alias)) {
				todoBatches.add(preSummary.aliasToTodoBatches.get(alias));
			} else {
				List<Integer> curTodoBatches = new ArrayList<Integer>();
				for (int batchCtr=0; batchCtr<nrBatches[aliasCtr]; ++batchCtr) {
					curTodoBatches.add(batchCtr);
				}
				todoBatches.add(curTodoBatches);				
			}
			// Initialize loaded batches
			currentBatches.add(new HashSet<Integer>());
			// Initialize reward scaling
			rewardScaling[aliasCtr] = 1;
		}
		System.out.println("Joined tables: " + 
				Arrays.toString(tables));
		System.out.println("Is temp table: " +
				Arrays.toString(isTempTable));
		//System.out.println("Cardinalities: " + Arrays.toString(cardinalities));
		//System.out.println("Batch sizes: " + Arrays.toString(batchSizes));
		System.out.println("Nr. batches: " +
				Arrays.toString(nrBatches));
		// Create tables holding tuple batches
		for (int aliasCtr=0; aliasCtr<nrJoined; ++aliasCtr) {
			createBatchTable(aliasCtr);
		}
		// Create table holding join result
		createResultTable();
		// Load initial tuple batches
		//nrCachedRows = new int[nrJoined];
		for (int aliasCtr=0; aliasCtr<nrJoined; ++aliasCtr) {
			pickBatches(aliasCtr, 1);
			if (JoinConfig.materializeBatches) {
				//nrCachedRows[aliasCtr] = 
				materializeBatches(aliasCtr);
			}
		}
		// Deactivate original optimizer
		PgConnector.disableJoinOrderOptimization();
		// Configure for batchwise execution
		PgConnector.enableBatchConfiguration();
	}
	/**
	 * Create table that will hold the next batch to process
	 * from given alias.
	 * 
	 * @param aliasCtr		ID of join item for which to create batch table
	 * @throws Exception
	 */
	void createBatchTable(int aliasCtr) throws Exception {
		String alias = query.aliases[aliasCtr];
		StringBuilder sqlBuilder = new StringBuilder();
		// Drop old batch table if any
		sqlBuilder.append("DROP TABLE IF EXISTS ");
		sqlBuilder.append(batchTables[aliasCtr]);
		sqlBuilder.append(";");
		PgConnector.update(sqlBuilder.toString());
		// Create batch table
		sqlBuilder = new StringBuilder();
		sqlBuilder.append("CREATE TEMP TABLE ");
		sqlBuilder.append(batchTables[aliasCtr]);
		sqlBuilder.append(" (");
		String columnList = preSummary.aliasToTypedNonIDcols.get(alias);
		sqlBuilder.append(columnList);
		if (!JoinConfig.deleteProcessed) {
			sqlBuilder.append(", ");
			sqlBuilder.append(NamingConfig.BATCH_ID_COLUMN);
			sqlBuilder.append(" INT");
		}
		/*
		Set<ColumnRef> allCols = new HashSet<ColumnRef>();
		allCols.addAll(query.colsForJoins);
		allCols.addAll(query.colsForPostProcessing);
		boolean firstCol = true;
		for (ColumnRef colRef : allCols) {
			if (alias.equals(colRef.aliasName)) {
				if (!firstCol) {
					sqlBuilder.append(", ");
				}
				sqlBuilder.append(colRef.columnName);
				sqlBuilder.append(" ");
				ColumnInfo colInfo = query.colRefToInfo.get(colRef);
				sqlBuilder.append(colInfo.columnType);
				firstCol = false;
			}
		}
		*/
		sqlBuilder.append(");");
		PgConnector.update(sqlBuilder.toString());
	}
	/**
	 * Create table to store intermediate join phase result.
	 * 
	 * @throws Exception
	 */
	void createResultTable() throws Exception {
		// Clean up from prior runs if necessary
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("DROP TABLE IF EXISTS ");
		sqlBuilder.append(joinResultTable);
		sqlBuilder.append(";");
		PgConnector.update(sqlBuilder.toString());
		// Create table holding join result
		sqlBuilder = new StringBuilder();
		sqlBuilder.append("CREATE TEMP TABLE ");
		sqlBuilder.append(joinResultTable);
		sqlBuilder.append(" (");
		// Iterate over columns required for next steps
		boolean firstCol = true;
		// Columns required for deduplication if applicable
		if (!JoinConfig.deleteProcessed) {
			for (ColumnRef colRef : query.colsForDedup) {
				if (!firstCol) {
					sqlBuilder.append(", ");
				}
				String resultColumnName = colRef.aliasName +
						"_" + colRef.columnName;
				sqlBuilder.append(resultColumnName);
				sqlBuilder.append(" INT");
				firstCol = false;
			}
		}
		// Columns required for post-processing
		for (ColumnRef colRef : query.colsForPostProcessing) {
			if (!firstCol) {
				sqlBuilder.append(", ");
			}
			String resultColumnName = colRef.aliasName + 
					"_" + colRef.columnName;
			sqlBuilder.append(resultColumnName);
			sqlBuilder.append(" ");
			// Get column type
			ColumnInfo colInfo = query.colRefToInfo.get(colRef);
			sqlBuilder.append(colInfo.columnType);
			joinResultColumns.add(resultColumnName);
			firstCol = false;
		}
		sqlBuilder.append(");");
		PgConnector.update(sqlBuilder.toString());
	}
	/**
	 * Creates SELECT clause for join query.
	 * 
	 * @param query			query to process
	 * @param preSummary	summary of pre-processing
	 * @return				string representing select clause
	 */
	String createSelectSQL(QueryInfo query, PreSummary preSummary) {
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("SELECT ");
		// Iterate over columns required for following steps
		boolean firstCol = true;
		// Columns for deduplication if applicable
		if (!JoinConfig.deleteProcessed) {
			for (ColumnRef colRef : query.colsForDedup) {
				if (!firstCol) {
					sqlBuilder.append(", ");
				}
				sqlBuilder.append(colRef);
				firstCol = false;
			}			
		}
		// Columns for post-processing
		for (ColumnRef colRef : query.colsForPostProcessing) {
			if (!firstCol) {
				sqlBuilder.append(", ");
			}
			sqlBuilder.append(colRef);
			firstCol = false;
		}
		sqlBuilder.append(" ");
		return sqlBuilder.toString();
	}
	/**
	 * Generates an SQL select clause for a sub-query
	 * retrieving one batch of a given table.
	 * 
	 * @param query			query to process
	 * @param preSummary	summary of pre-processing
	 * @param alias			alias of table to which sub-query refers
	 * @return				select clause for sub-query
	 */
	/*
	String createSubSelectSQL(QueryInfo query, 
			PreSummary preSummary, String alias) {
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("SELECT ");
		Set<ColumnRef> allCols = new HashSet<ColumnRef>();
		allCols.addAll(query.colsForJoins);
		allCols.addAll(query.colsForPostProcessing);
		boolean firstCol = true;
		for (ColumnRef colRef : allCols) {
			if (alias.equals(colRef.aliasName)) {
				if (!firstCol) {
					sqlBuilder.append(", ");
				}
				String columnName = colRef.columnName;
				sqlBuilder.append(columnName);
				firstCol = false;
			}
		}
		return sqlBuilder.toString();
	}
	*/
	/**
	 * Creates FROM clause with tables following
	 * the specified join order.
	 * 
	 * @param order	specifies join order
	 * @return		SQL string with ordered FROM clause
	 */
	String createFromSQL(int[] order) {
		StringBuilder sqlBuilder = new StringBuilder();
		int nrJoined = query.nrJoined;
		boolean firstItem = true;
		for (int itemCtr=0; itemCtr<nrJoined; ++itemCtr) {
			if (!firstItem) {
				sqlBuilder.append(", ");
			}
			int aliasIdx = order[itemCtr];
			String alias = query.aliases[aliasIdx];
			String table = tables[aliasIdx];
			sqlBuilder.append(table);
			sqlBuilder.append(" AS ");
			sqlBuilder.append(alias);
			firstItem = false;
		}
		return sqlBuilder.toString();
	}
	/**
	 * Creates a conjunction of all join predicates.
	 * 
	 * @param query			query to process
	 * @param preSummary	summary of pre-processing
	 * @return				string representing join predicate
	 */
	String createWhereSQL(QueryInfo query, 
			PreSummary preSummary) {
		StringBuilder sqlBuilder = new StringBuilder();
		boolean firstPred = true;
		for (ExpressionInfo joinPred : query.joinPredicates) {
			if (!firstPred) {
				sqlBuilder.append(" AND ");
			}
			sqlBuilder.append(joinPred.finalExpression);
			firstPred = false;
		}
		return sqlBuilder.toString();
	}
	/**
	 * Generates SQL where clause selecting tuples
	 * in any of the specified batches.
	 * 
	 * @param sourceTable	load tuples from this table
	 * @param batchIDs		select tuples from those batches
	 * @return				SQL string representing condition
	 */
	String whereForBatches(String sourceTable, Collection<Integer> batchIDs) {
		StringBuilder sqlBuilder = new StringBuilder();
		// This assumes that ctid remains constant for the time
		// of query execution! Alternatively, use hash value of
		// entire row.
		/*
		sqlBuilder.append(" WHERE (ctid::text::point)[1]::int % ");
		sqlBuilder.append(nrBatches[tableIdx]);
		*/
		//sqlBuilder.append(" WHERE ");
		sqlBuilder.append(sourceTable);
		sqlBuilder.append(".");
		sqlBuilder.append(NamingConfig.BATCH_ID_COLUMN);
		sqlBuilder.append(" IN (");
		boolean first = true;
		for (int batchID : batchIDs) {
			if (!first) {
				sqlBuilder.append(", ");
			}
			sqlBuilder.append(batchID);
			first = false;
		}
		sqlBuilder.append(")");
		return sqlBuilder.toString();
	}
	/**
	 * Randomly select at most given number of
	 * batches to treat next for given table
	 * (less batches if the total number of 
	 * remaining batches is lower than requested).
	 * 
	 * @param tableIdx					pick batches for that table
	 * @param nrBatchesRequested		pick at most that many batches
	 */
	void pickBatches(int tableIdx, int nrBatchesRequested) {
		// Select random batch indices
		List<Integer> curTodoBatches = todoBatches.get(tableIdx);
		int nrBatchesAvailable = curTodoBatches.size();
		Set<Integer> curLoadedBatches = new HashSet<Integer>();
		currentBatches.set(tableIdx, curLoadedBatches);
		if (LogConfig.VERBOSE) {
			System.out.println("Nr. batches requested: " + nrBatchesRequested);
			System.out.println("Nr. batches available: " + nrBatchesAvailable);			
		}
		nrBatchesRequested = Math.min(nrBatchesRequested, nrBatchesAvailable);
		while (curLoadedBatches.size() < nrBatchesRequested) {
			int batchPos = random.nextInt(nrBatchesAvailable);
			int batchId = curTodoBatches.get(batchPos);
			curLoadedBatches.add(batchId);
		}
		if (LogConfig.VERBOSE) {
			System.out.println("Selected batches to load");				
		}
	}
	/**
	 * Replaces tuple cache content for given table
	 * by tuples from currently selected batches.
	 * 
	 * @param tableIdx	materialize current tuples for that table
	 * @return number of materialized rows
	 */
	int materializeBatches(int tableIdx) throws Exception {
		String table = tables[tableIdx];
		String batchTable = batchTables[tableIdx];
		// Reset timeout
		PgConnector.setNoTimeout();
		// Check whether it makes sense to create an index on batch ID
		if (JoinConfig.batchIDindexThreshold < 1.0 && 
				todoBatches.get(tableIdx).size() < nrBatches[tableIdx] * 
				JoinConfig.batchIDindexThreshold) {
			StringBuilder sqlBuilder = new StringBuilder();
			sqlBuilder.append("CREATE INDEX IF NOT EXISTS ");
			sqlBuilder.append(table + "_batchNrIndex");
			sqlBuilder.append(" ON ");
			sqlBuilder.append(table);
			sqlBuilder.append(" (");
			sqlBuilder.append(NamingConfig.BATCH_ID_COLUMN);
			sqlBuilder.append(");");
			PgConnector.update(sqlBuilder.toString());
		}
		// Remove tuples from already processed batches
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder = new StringBuilder();
		sqlBuilder.append("TRUNCATE ");
		sqlBuilder.append(batchTable);
		sqlBuilder.append(";");
		PgConnector.update(sqlBuilder.toString());
		// Obtain batches to materialize
		Set<Integer> toLoad = currentBatches.get(tableIdx);
		// Load tuples from those batches into cache
		if (toLoad.size() > 0) {
			sqlBuilder = new StringBuilder();
			sqlBuilder.append("INSERT INTO ");
			sqlBuilder.append(batchTable);
			sqlBuilder.append(" (SELECT ");
			String alias = query.aliases[tableIdx];
			String columnList = preSummary.aliasToNonIDcols.get(alias);
			sqlBuilder.append(columnList);
			// Need batch ID for deduplication if processed
			// batches are not deleted from joined tables.
			if (!JoinConfig.deleteProcessed) {
				sqlBuilder.append(", ");
				sqlBuilder.append(NamingConfig.BATCH_ID_COLUMN);
			}
			sqlBuilder.append(" FROM ");
			sqlBuilder.append(table);
			sqlBuilder.append(" AS ");
			sqlBuilder.append(alias);
			sqlBuilder.append(" WHERE ");
			sqlBuilder.append(whereForBatches(alias, toLoad));
			ExpressionInfo unaryPred = preSummary.aliasToUnaryTodo.get(alias);
			if (unaryPred != null) {
				sqlBuilder.append(" AND ");
				sqlBuilder.append(unaryPred.toString());
			}
			sqlBuilder.append(");");
			return PgConnector.update(sqlBuilder.toString());
		} else {
			return 0;
		}
	}
	/**
	 * Returns the ID of the first non-empty batch for
	 * the given table or -1 if no such batch exists.
	 * 
	 * @param tableIdx	find batches for this table
	 * @return			ID of non-empty batch or -1
	 * @throws Exception
	 */
	int findNonEmptyBatch(int tableIdx) throws Exception {
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("SELECT ");
		sqlBuilder.append(NamingConfig.BATCH_ID_COLUMN);
		sqlBuilder.append(" FROM ");
		sqlBuilder.append(tables[tableIdx]);
		sqlBuilder.append(" AS ");
		String alias = query.aliases[tableIdx];
		sqlBuilder.append(alias);
		sqlBuilder.append(" WHERE ");
		sqlBuilder.append(whereForBatches(alias, todoBatches.get(tableIdx)));
		ExpressionInfo unaryTodo = preSummary.aliasToUnaryTodo.get(alias);
		if (unaryTodo != null) {
			sqlBuilder.append(" AND ");
			sqlBuilder.append(unaryTodo.toString());
		}
		sqlBuilder.append(" LIMIT 1;");
		ResultSet result = PgConnector.query(sqlBuilder.toString());
		if (result.next()) {
			return result.getInt(1);
		} else {
			return -1;
		}
	}
	/**
	 * Materialize currently selected batches, select new
	 * batches and materialize until tuple cache is non-empty
	 * or all batches of current table are processed.
	 * 
	 * @param tableIdx		fill tuple cache for that table
	 * @throws Exception
	 */
	void fillTupleCache(int tableIdx) throws Exception {
		int iterationCtr = 0;
		while (materializeBatches(tableIdx) == 0 &&
				!todoBatches.get(tableIdx).isEmpty()) {
			/*
			if (iterationCtr % 500 == 0) {
				System.out.println("Table " + tableIdx + " remaining: " + 
						todoBatches.get(tableIdx).size());
			}
			*/
			++iterationCtr;
			finalizeCurrentBatches(tableIdx);
			pickBatches(tableIdx, JoinConfig.defaultLoadNr);
			// Start search for non-empty batches if iteration
			// threshold is reached.
			if (iterationCtr >= JoinConfig.fillTriesBeforeSearch) {
				int nextBatch = findNonEmptyBatch(tableIdx);
				if (nextBatch == -1) {
					todoBatches.get(tableIdx).clear();
					break;
				} else {
					currentBatches.get(tableIdx).clear();
					currentBatches.get(tableIdx).add(nextBatch);
				}
			}
		}
	}
	/**
	 * Invoked if the current batch of tuples
	 * was successfully processed. Removes loaded
	 * batch indexes from todo list and removes
	 * associated tuples from source table.
	 * 
	 * @param tableIdx	index of table whose tuples
	 * 					were successfully processed
	 */
	void finalizeCurrentBatches(int tableIdx) throws Exception {
		String table = tables[tableIdx];
		Set<Integer> curLoadedBatches = currentBatches.get(tableIdx);
		// Remove tuples from table if enabled
		if (JoinConfig.deleteProcessed) {
			StringBuilder sqlBuilder = new StringBuilder();
			sqlBuilder.append("DELETE FROM ");
			sqlBuilder.append(table);
			sqlBuilder.append(" WHERE ");
			sqlBuilder.append(whereForBatches(table, curLoadedBatches));
			sqlBuilder.append(";");
			PgConnector.update(sqlBuilder.toString());			
		}
		// Remove batches from todo list
		todoBatches.get(tableIdx).removeAll(curLoadedBatches);
		// Update list of loaded batches
		curLoadedBatches.clear();
	}
	/**
	 * Calculate for each table how much one processed batch is worth.
	 * It is worth more if few batches remain (since it represents a
	 * higher percentage of the total remaining work).
	 */
	void updateRewardScaling() {
		int nrJoined = query.nrJoined;
		int minNrRemaining = Integer.MAX_VALUE;
		for (int tableCtr=0; tableCtr<nrJoined; ++tableCtr) {
			int nrRemaining = todoBatches.get(tableCtr).size();
			minNrRemaining = Math.min(minNrRemaining, nrRemaining);
		}
		for (int tableCtr=0; tableCtr<nrJoined; ++tableCtr) {
			int nrRemaining = todoBatches.get(tableCtr).size();
			rewardScaling[tableCtr] = (double)minNrRemaining / nrRemaining;
		}
	}
	/**
	 * Generates a query that adds one more result fragment
	 * to the join result according to the given join order.
	 * 
	 * @param order		join order
	 * @return			SQL query adding one result fragment
	 */
	String addToResultQuery(int[] order) {
		// Get alias and table of first item in join order
		int firstIdx = order[0];
		String firstAlias = query.aliases[firstIdx];
		String firstTable = JoinConfig.materializeBatches?
				batchTables[firstIdx]:tables[firstIdx];
		// Generate query joining one batch with given join order
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder = new StringBuilder();
		sqlBuilder.append("INSERT INTO ");
		sqlBuilder.append(joinResultTable);
		sqlBuilder.append(" (");
		sqlBuilder.append(selectSQL);
		sqlBuilder.append(" FROM ");
		int nrJoined = query.nrJoined;
		for (int joinCtr=0; joinCtr<nrJoined; ++joinCtr) {
			int table = order[joinCtr];
			// First table receives special treatment
			if (joinCtr == 0) {
				sqlBuilder.append(firstTable);
				sqlBuilder.append(" AS ");
				sqlBuilder.append(firstAlias);
			} else {
				sqlBuilder.append(" CROSS JOIN ");
				sqlBuilder.append(tables[table]);
				sqlBuilder.append(" AS ");
				sqlBuilder.append(query.aliases[table]);
			}
		}
		// Create WHERE clause
		sqlBuilder.append(" WHERE ");
		List<String> whereFrags = new ArrayList<String>();
		whereFrags.add(whereSQL);
		for (int joinCtr=1; joinCtr<nrJoined; ++joinCtr) {
			String alias = query.aliases[order[joinCtr]];
			ExpressionInfo unaryPred = preSummary.aliasToUnaryTodo.get(alias);
			if (unaryPred != null) {
				whereFrags.add(unaryPred.toString());
			}
		}
		if (!JoinConfig.materializeBatches) {
			Set<Integer> batches = currentBatches.get(firstIdx);
			String batchPred = whereForBatches(firstAlias, batches);
			whereFrags.add(batchPred);
		}
		sqlBuilder.append(StringUtils.join(whereFrags, " AND "));
		sqlBuilder.append(");");
		return sqlBuilder.toString();
	}
	/**
	 * Execute given join order for given amount of time,
	 * taking only the content of the current tuple cache
	 * instead of the full first table in the join order.
	 * 
	 * @param order				join order
	 * @param timeoutMillis		milliseconds until timeout
	 * @return	true iff execution finished
	 */
	public boolean execute(int[] order, int timeoutMillis) 
			throws Exception {
		// Output join order
		if (LogConfig.VERBOSE) {
			System.out.println("Executing order: " + Arrays.toString(order));			
		}
		// First table in join order
		int firstIdx = order[0];
		// Whether first batch was processed until timeout
		boolean firstBatchSuccess = false;
		// Number of batches we try to process at once
		int nrBatchesPerTry = JoinConfig.defaultLoadNr;
		// Multiply timeout by this scaling factor
		double timeoutFactor = 1;
		//double timeoutFactor = Math.pow((double)nrBatches[firstIdx] /Math.max(todoBatches.get(firstIdx).size(), 1), 0.5);
		// Execute join order with timeout - if successful,
		// increase number of batches and timeout until
		// processing finishes or a timeout is reached.
		boolean success = true;
		// How many greedy iterations with the same, successful, join order
		int roundCtr=0;
		while (!finished && success && roundCtr<JoinConfig.maxGreedyExecutions) 
		{
			++roundCtr;
			//System.out.println(roundCtr);
			success = false;
			int updatedTimeout = (int)Math.round(timeoutMillis * timeoutFactor);
			if (JoinConfig.hardTimeout) {
				PgConnector.setTimeout(updatedTimeout);				
			} else {
				PgConnector.setNoTimeout();
			}
			String querySQL = addToResultQuery(order);
			try {
				long queryStartMillis = System.currentTimeMillis();
				PgConnector.update(querySQL);
				long queryTotalMillis = System.currentTimeMillis() - queryStartMillis;
				// Success with current batch
				success = true;
				// At least one processed batch
				if (JoinConfig.hardTimeout) {
					firstBatchSuccess = true;
				} else if (roundCtr==1) {
					firstBatchSuccess = queryTotalMillis <= updatedTimeout;
				}
			} catch (SQLTimeoutException e) {
	        } catch (PSQLException | SQLRecoverableException e) {
	            if (e.toString().contains("timeout") || 
	            		e.toString().contains("timed out")) {
	            } else {
	            	throw e;
	            }
			}
			if (LogConfig.VERBOSE) {
				if (success) {
					System.out.println("No timeout.");
				} else {
					System.out.println("Had timeout!");
				}
			}
			PgConnector.setNoTimeout();
			// Collect stats
			if (success) {
				int nrSuccesses = JoinStats.timeoutToNrSuccesses.containsKey(updatedTimeout)?
						JoinStats.timeoutToNrSuccesses.get(updatedTimeout):0;
				JoinStats.timeoutToNrSuccesses.put(updatedTimeout, nrSuccesses + 1);
			}
			int nrTries = JoinStats.timeoutToNrTries.containsKey(updatedTimeout)?
					JoinStats.timeoutToNrTries.get(updatedTimeout):0;
			JoinStats.timeoutToNrTries.put(updatedTimeout, nrTries + 1);
			// Potentially replace batch even if it was not processed
			if (!success) {
				if (nrBatchesPerTry > 1 || random.nextDouble() < 
						JoinConfig.batchReloadProbability) {
					pickBatches(firstIdx, JoinConfig.defaultLoadNr);
					if (JoinConfig.materializeBatches) {
						fillTupleCache(firstIdx);
					}
				}
				return firstBatchSuccess;
			}
			// Update reward scaling factors
			updateRewardScaling();
			// Mark tuples in tuple batch as processed
			finalizeCurrentBatches(firstIdx);
			// Check for termination
			if (todoBatches.get(firstIdx).isEmpty()) {
				// Set termination flag
				finished = true;
				// Make join result persistent if in debugging mode
				if (LogConfig.DEBUG_MODE) {
					PgConnector.update("DROP TABLE IF EXISTS SkinnerJoinOutputDebug;");
					PgConnector.update("CREATE TABLE SkinnerJoinOutputDebug " + 
							"AS (SELECT * FROM " + joinResultTable + ");");
				}
			} else {
				// Output progress update
				++nrProgressUpdates;
				if (nrProgressUpdates % LogConfig.logProgressEvery == 0) {
					System.out.println("*** Progress report: remaining batches ***");
					System.out.println(String.join("\t", query.aliases));
					int nrJoined = order.length;
					for (int aliasCtr=0; aliasCtr<nrJoined; ++aliasCtr) {
						int remainingPercent = (100 * todoBatches.get(aliasCtr).size()) / nrBatches[aliasCtr];
						System.out.print(remainingPercent + "%\t");
					}
					System.out.println();
					for (int aliasCtr=0; aliasCtr<nrJoined; ++aliasCtr) {
						int remaining = todoBatches.get(aliasCtr).size();
						System.out.print(remaining + "\t");
					}
					System.out.println();
					System.out.println("Join order:\t" + Arrays.toString(order));
					//System.out.println("Timeout:\t" + (timeoutMillis * timeoutFactor));
					// Print join stats
					System.out.println("*** Join stats: successful batches per timeout ***");
					JoinStats.print();
					System.out.println("***");					
				}
				// Load new data into tuple batch
				pickBatches(firstIdx, nrBatchesPerTry);
				if (JoinConfig.materializeBatches) {
					fillTupleCache(firstIdx);					
				}
				// No timeout -> try again with more time and more data
				nrBatchesPerTry *= JoinConfig.greedyBatchScaleUp;
				nrBatchesPerTry = Math.min(nrBatchesPerTry, JoinConfig.greedyMaxBatchScaleUp);
				timeoutFactor *= JoinConfig.greedyTimeScaleUp;
				timeoutFactor = Math.min(timeoutFactor, JoinConfig.greedyMaxTimeScaleUp);
			}
			// No further executions if timeout is too high
			if (updatedTimeout > JoinConfig.greedyExecutionThreshold) {
				break;
			}
		}
		// We consider this join order a success if first
		// data batch was processed using original timeout.
		return firstBatchSuccess;
	}
}
