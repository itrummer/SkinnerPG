package joining;

import java.util.Arrays;

import config.JoinConfig;
import config.LogConfig;
import config.NamingConfig;
import connector.PgConnector;
import optimizer.uct.UctNode;
import preprocessing.PreSummary;
import query.QueryInfo;
import statistics.GeneralStats;
import statistics.JoinStats;

/**
 * Executes joins between data batches, taken from
 * pre-filtered tables (during pre-processing).
 * Uses reinforcement learning to converge to
 * good join orders.
 * 
 * @author immanueltrummer
 *
 */
public class JoinProcessor {
	/**
	 * Selects timeout for next execution episode, given
	 * a set of available timeouts and accumulated
	 * execution time spent with each timeout.
	 * 
	 * @param timeouts			available timeouts to choose from
	 * @param accumulatedTime	accumulated time for each timeout
	 * @return					next timeout level
	 */
	static int nextTimeout(int[] timeouts, int[] accumulatedTime) {
		int nrTimeouts = timeouts.length;
		for (int timeCtr=nrTimeouts-1; timeCtr>=0; --timeCtr) {
			// Iterate over smaller timeouts
			boolean admissible = true;
			for (int smallerTimesCtr=0; smallerTimesCtr<timeCtr; 
					++smallerTimesCtr) {
				if (accumulatedTime[smallerTimesCtr] < 
						accumulatedTime[timeCtr] + timeouts[timeCtr]) {
					admissible = false;
					break;
				}
			}
			// Check if current timeout is ok
			if (admissible) {
				accumulatedTime[timeCtr] += timeouts[timeCtr];
				return timeCtr;
			}
		}
		return -1;
	}
	/**
	 * Generates one SQL query that would conclude the join phase
	 * if its execution is possible within the current time budget.
	 * The query is formulated such that the original optimizer may
	 * do join re-ordering.
	 * 
	 * @param query				query to process
	 * @param preSummary		summary of steps during pre-processing
	 * @param joinResultTable	name of table holding join result
	 * @param executor			UCT executor (we can re-use query fragments
	 * 							generated during its initialization)
	 * @return	SQL query that would finish processing
	 */
	static String traditionalQuery(QueryInfo query, PreSummary preSummary, 
			String joinResultTable, BatchedExecutor executor) {
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("INSERT INTO ");
		sqlBuilder.append(joinResultTable);
		sqlBuilder.append(" (");
		sqlBuilder.append(executor.selectSQL);
		sqlBuilder.append(" FROM ");
		int nrJoined = query.nrJoined;
		for (int joinCtr=0; joinCtr<nrJoined; ++joinCtr) {
			if (joinCtr > 0) {
				sqlBuilder.append(", ");
			}
			sqlBuilder.append(executor.tables[joinCtr]);
			sqlBuilder.append(" AS ");
			sqlBuilder.append(query.aliases[joinCtr]);
		}
		sqlBuilder.append(" WHERE ");
		sqlBuilder.append(executor.whereSQL);
		sqlBuilder.append(");");
		return sqlBuilder.toString();
	}
	/**
	 * Creates first a temporary table to
	 * store join results (which will form
	 * the input for the post-processing
	 * stage). Then, joins small data
	 * batches according to different
	 * join orders, using the UCT algorithm
	 * to choose the join order to try next.
	 * 
	 * @param query			query to process
	 * @param preSummary	summary of pre-processing
	 * @param queryID		query ID used for naming intermediate relations
	 * @return				summary of join phase
	 */
	public static JoinSummary process(QueryInfo query, 
			PreSummary preSummary, String queryID) throws Exception {
		System.out.println("Starting join processing ...");
		// Re-initialize statistics
		GeneralStats.lastNonBatchedTime = -1;
		// Generate names for intermediate result relations
		String joinResultTable = NamingConfig.JOIN_TBL + queryID;
		String finalResultTable = NamingConfig.FINAL_TBL + queryID;
		PgConnector.dropTable(joinResultTable);
		PgConnector.dropTable(finalResultTable);
		// Prepare join executor
		BatchedExecutor executor = new BatchedExecutor(
				query, preSummary, joinResultTable);
		// Get configuration parameters
		int nrTimeouts = JoinConfig.nrTimeouts;
		int base = JoinConfig.timeoutBase;
		double factor = JoinConfig.timeoutScaleUp;
		// Prepare pyramid timeout scheme
		int[] timeouts = new int[nrTimeouts];
		int[] accumulatedTime = new int[nrTimeouts];
		for (int timeCtr=0; timeCtr<nrTimeouts; ++timeCtr) {
			timeouts[timeCtr] = (int)Math.round(base * 
					Math.pow(factor, timeCtr));
		}
		// Create UCT root node
		long roundCtr = 0;
		UctNode uctRoot = new UctNode(
				0, query, true, executor);
		// Initialize join stats
		JoinStats.init();
		// Execute until join phase finished
		int roundsToSwitch = JoinConfig.initialRoundsToSwitch;
		int[] order = new int[query.nrJoined];
		boolean allFinished = false;
		while (!executor.finished && !allFinished) {
			// Try execution one by one on small data batches
			PgConnector.enableBatchConfiguration();
			long batchedStartRound = roundCtr;
			long batchedStartMillis = System.currentTimeMillis();
			while (!executor.finished && 
					roundCtr - batchedStartRound <= roundsToSwitch) {
				++roundCtr;
				int level = nextTimeout(timeouts, accumulatedTime);
				int timeout = timeouts[level];
				uctRoot.sample(roundCtr, order, timeout);
				if (roundCtr > JoinConfig.softenTimeoutAfter) {
					JoinConfig.hardTimeout = false;
				}
				// Print out dominant join order
				if (roundCtr % LogConfig.logDominantEvery == 0) {
					int[] domOrder = uctRoot.dominantOrder();
					System.out.println("Dominant order:\t" + 
							Arrays.toString(domOrder));
				}
			}
			// Follow up with non-batched execution if not finished
			if (!executor.finished) {
				long startMillis = System.currentTimeMillis();
				long totalBatchedMillis = Math.max(1, 
						System.currentTimeMillis() - batchedStartMillis);
				System.out.println("Start millis: " + batchedStartMillis);
				System.out.println("Total millis: " + totalBatchedMillis);
				// Try execution without batching
				PgConnector.disableBatchConfiguration();
				int[] dominantOrder = uctRoot.dominantOrder();
				String reorderedQuery = query.reorderedQuery(dominantOrder);
				String createResultSQL = "CREATE TEMP TABLE " + finalResultTable + 
						" AS (" + reorderedQuery + ");";
				System.out.println("Query with joins reordered according to dominant order:");
				System.out.println(reorderedQuery);
				if (JoinConfig.switchBackToBatchMode) {
					System.out.println("Trying non-batched execution for " + 
							totalBatchedMillis + " ms ...");
					allFinished = PgConnector.updateOrTimeout(
							createResultSQL, (int)totalBatchedMillis);					
				} else {
					System.out.println("Executing without batching.");
					PgConnector.setNoTimeout();
					PgConnector.update(createResultSQL);
					allFinished = true;
				}
				// record time for non-batched execution
				GeneralStats.lastNonBatchedTime = 
						System.currentTimeMillis() - startMillis;
				if (allFinished) {
					System.out.println("Non-batched execution successful!");
				} else {
					System.out.println("Non-batched execution failed - back to batch mode ...");
				}
				// Scale up budget per approach
				roundsToSwitch *= JoinConfig.roundsToSwitchScaleUp;				
			}
		}
		// Return summary
		return new JoinSummary(joinResultTable, 
				executor.joinResultColumns, allFinished);
	}
}
