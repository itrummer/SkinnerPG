package statistics;

/**
 * Stores execution statistics that are not
 * specific to any of the three phases.
 * 
 * @author immanueltrummer
 *
 */
public class GeneralStats {
	/**
	 * Total execution time (in milliseconds) for
	 * last executed query.
	 */
	public static long lastExecutionTime = -1;
	/**
	 * Milliseconds for last non-batched query
	 * execution if any.
	 */
	public static long lastNonBatchedTime = -1;
	/**
	 * Whether intra-query learning was used for
	 * the last query execution.
	 */
	public static boolean lastUsedLearning = false;
}
