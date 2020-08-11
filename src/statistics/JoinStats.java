package statistics;

import java.util.HashMap;
import java.util.Map;

/**
 * Statistics collected curing the join phase.
 * 
 * @author immanueltrummer
 *
 */
public class JoinStats {
	/**
	 * Maps timeout (in milliseconds) to number of
	 * successfully processed batches with that timeout.
	 */
	public static Map<Integer, Integer> timeoutToNrSuccesses;
	/**
	 * Maps timeout (in milliseconds) to number of tries.
	 */
	public static Map<Integer, Integer> timeoutToNrTries;
	/**
	 * Resets all the counters.
	 */
	public static void init() {
		timeoutToNrSuccesses = new HashMap<Integer, Integer>();
		timeoutToNrTries = new HashMap<Integer, Integer>();
	}
	/**
	 * Milliseconds for join phase during last query execution.
	 */
	public static long lastMillis = -1;
	/**
	 * Prints all counters to standard output.
	 */
	public static void print() {
		System.out.println(timeoutToNrSuccesses.toString());
		System.out.println(timeoutToNrTries.toString());
	}
}
