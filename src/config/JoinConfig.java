package config;

/**
 * Configuration parameters for the join
 * processing phase.
 * 
 * @author immanueltrummer
 *
 */
public class JoinConfig {
	/**
	 * Whether to restart query processing until the allocated
	 * time budget for optimization is depleted. This is only
	 * useful if we want to analyze the query (as opposed to
	 * processing it as fast as possible).
	 */
	public static final boolean restartForAnalysis = true;
	/**
	 * Choose reinforcement learning algorithm used.
	 */
	public static final LearningAlg learningAlg = LearningAlg.BRUE;
	/**
	 * Whether to enable nested loop join in non-batched processing mode.
	 */
	public static final boolean enableNestLoopNonBatched = true;
	/**
	 * Number of tuple batches per table.
	 */
	public static final int nrBatches = 10000;
	/**
	 * How often do we try to find non-empty tuple batches for a
	 * base table before searching systematically for non-empty batches?
	 */
	public static final int fillTriesBeforeSearch = 5;
	/**
	 * Whether to issue queries to retrieve IDs of non-empty
	 * batches for filtered tables during pre-processing.
	 */
	public static final boolean filterBatchIDs = true;
	/**
	 * Whether to delete processed batches from the input
	 * (if not, a deduplication step is performed between
	 * join phase and the start of post-processing).
	 */
	public static final boolean deleteProcessed = false;
	/**
	 * Whether to materialize tuple batches after extraction
	 * (this creates overheads but avoids reloading the same
	 * batch multiple times if processing is unsuccessful).
	 */
	public static final boolean materializeBatches = true;
	/**
	 * Whether query processing stops at timeout (or whether
	 * timeout influences only the reward calculation).
	 */
	public static boolean hardTimeout = true;
	/**
	 * Transform hard into soft timeout after that many
	 * rounds played.
	 */
	public static final long softenTimeoutAfter = Integer.MAX_VALUE;
	/**
	 * Start timeout used (in milliseconds).
	 */
	public static final int timeoutBase = 20;
	/**
	 * Factor by which timeout if scaled up
	 * for the next timeout level.
	 */
	public static final double timeoutScaleUp = 2;
	/**
	 * How many batches to load at once by default.
	 */
	public static final int defaultLoadNr = 1;
	/**
	 * Number of different timeouts that are
	 * considered.
	 */
	public static final int nrTimeouts = 7;
	/**
	 * Multiply exploration term by that factor (use sqrt(2)
	 * as theoretical recommendation, lowering increases
	 * exploitation and can improve performance though).
	 */
	public static final double explorationFactor = Math.sqrt(2); 
	/**
	 * If the ratio of remaining tuples for a table is
	 * below that threshold (i.e., the table is often
	 * used as left-most table in join order), we create
	 * an index on the batch ID column. Setting this
	 * parameter to zero avoids creating indexes.
	 * If set to one, indexes are created during
	 * pre-processing.
	 */
	public static final double batchIDindexThreshold = 1;
	/**
	 * Probability that a new random data batch is loaded
	 * for a given table without the current batch having
	 * been processed. A value of 0.1 works well for run
	 * time optimization, set to 1.0 for offline optimization.
	 */
	public final static double batchReloadProbability = 0.1;
	/**
	 * Maximal timeout (millis) at which we re-execute immediately
	 * with a successful join order.
	 */
	public static final int greedyExecutionThreshold = 40;
	/**
	 * Maximal number of consecutive executions using
	 * the same join order without using UCT. One is
	 * the minimum.
	 */
	public static final int maxGreedyExecutions = 1;
	/**
	 * If a batch was processed successfully with
	 * a given join order, we process more batches
	 * using the same join order - this factor
	 * determines how any batches we try next.
	 */
	public static final int greedyTimeScaleUp = 5;
	/**
	 * Scale up number of batches per execution by
	 * that factor when executing the same, 
	 * successful, join order greedily.
	 */
	public static final int greedyBatchScaleUp = 5;
	/**
	 * Maximal factor by which timeout is scaled
	 * up during greedy executions.
	 */
	public static final int greedyMaxTimeScaleUp = 5;
	/**
	 * Maximal factor by which batch size is scaled up
	 * during greedy executions.
	 */
	public static final int greedyMaxBatchScaleUp = 5;
	/**
	 * How many rounds to play initially before switching
	 * from batched to non-batched execution (with dominant
	 * join order).
	 */
	public static final int initialRoundsToSwitch = 5000;
	/**
	 * Whether to switch back to batch execution mode if
	 * non-batched execution of most promising plan does
	 * not succeed within timeout (timeout is the time
	 * spent in the last batched execution phase).
	 */
	public static final boolean switchBackToBatchMode = false;
	/**
	 * Multiply rounds to switch by that factor after each
	 * switch.
	 */
	public static final int roundsToSwitchScaleUp = 2;
}
