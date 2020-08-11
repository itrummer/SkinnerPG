package execution;

import config.JoinConfig;
import config.MasterConfig;
import config.NamingConfig;
import connector.PgConnector;
import joining.JoinProcessor;
import joining.JoinSummary;
import net.sf.jsqlparser.statement.select.PlainSelect;
import optimizer.brue.JoinProcessorBrue;
import postprocessing.PostProcessor;
import preprocessing.PreSummary;
import preprocessing.Preprocessor;
import query.QueryInfo;
import statistics.GeneralStats;
import statistics.JoinStats;
import statistics.PostStats;
import statistics.PreStats;

/**
 * Controls the high-level query execution workflow.
 * 
 * @author immanueltrummer
 *
 */
public class Master {
	/**
	 * Executes given query using intra-query learning if
	 * configured conditions are satisfied. Stores
	 * query result in database and execution statistics
	 * in memory.
	 * 
	 * @param plainSelect	the query to process
	 * @param queryID		query ID (used to name intermediate
	 * 						result relations in database).
	 * @return	returns name of relation containing query result
	 * @throws Exception
	 */
	public static String execute(PlainSelect plainSelect, 
			String queryID) throws Exception {
		// Query result will be stored in this table
		String finalTable = NamingConfig.FINAL_TBL + queryID;
		// Try standard execution first
		long startMillis = System.currentTimeMillis();
		boolean noLearning = executeNoLearning(plainSelect, finalTable);
		GeneralStats.lastUsedLearning = !noLearning;
		if (!noLearning) {
			QueryInfo query = new QueryInfo(plainSelect);
			// Pre-processing
			long preStart = System.currentTimeMillis();
			PreSummary preSummary = Preprocessor.process(query);
			PreStats.lastMillis = System.currentTimeMillis() - preStart;
			System.out.println(preSummary.toString());
			// Join processing
			long joinStart = System.currentTimeMillis();
			JoinSummary joinSummary = null;
			switch (JoinConfig.learningAlg) {
			case BRUE:
				joinSummary = JoinProcessorBrue.process(
						query, preSummary, queryID);
				break;
			case UCT:
				joinSummary = JoinProcessor.process(
						query, preSummary, queryID);
				break;
			}
			JoinStats.lastMillis = System.currentTimeMillis() - joinStart;
			// Post-processing
			long postStart = System.currentTimeMillis();
			if (!joinSummary.finishedPostProceccing) {
				PostProcessor.process(query, joinSummary, queryID);
			}
			PostStats.lastMillis = System.currentTimeMillis() - postStart;
		} else {
			PreStats.lastMillis = 0;
			JoinStats.lastMillis = 0;
			PostStats.lastMillis = 0;
		}
		GeneralStats.lastExecutionTime = System.currentTimeMillis() - startMillis;
		return finalTable;
	}
	/**
	 * Executes query with timeout using the plan proposed by
	 * the traditional optimizer. Returns flag indicating
	 * whether execution succeeded within timeout.
	 * 
	 * @param plainSelect	query to process
	 * @param finalTable	store result in this table
	 * @return				true iff execution succeeded
	 * @throws Exception
	 */
	static boolean executeNoLearning(PlainSelect plainSelect, 
			String finalTable) throws Exception {
		if (MasterConfig.learningTimeThreshold > 0) {
			// Try executing query traditionally until timeout is reached
			PgConnector.enableJoinOrderOptimization();
			PgConnector.disableBatchConfiguration();
			// Remove result table if any
			PgConnector.dropTable(finalTable);
			// Construct full query
			String sql = "CREATE TEMP TABLE " + finalTable + 
					" AS (" + plainSelect.toString() + ");";
			return PgConnector.updateOrTimeout(sql, 
					MasterConfig.learningTimeThreshold);	
		} else {
			return false;
		}
	}
}
