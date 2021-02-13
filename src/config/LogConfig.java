package config;

/**
 * Configures output on standard console
 * and creation of persistent intermediate
 * result tables in the database that can
 * be inspected after execution.
 * 
 * @author immanueltrummer
 *
 */
public class LogConfig {
	/**
	 * Produces particularly verbose output 
	 * and stores intermediate results in
	 * database for debugging.
	 */
	public static final boolean DEBUG_MODE = false;
	/**
	 * Whether to produce verbose output
	 * (careful, can impact performance!).
	 */
	public static final boolean VERBOSE = true;
	/**
	 * Print out every i-th progress update message.
	 */
	public static final int logProgressEvery = 1000;
	/**
	 * Print out dominant join order every i-th iteration.
	 */
	public static final int logDominantEvery = 1;
}
