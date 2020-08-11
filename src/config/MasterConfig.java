package config;

/**
 * Parameters configuring the high-level workflow
 * executed by the Master (e.g., parameters specifying
 * the conditions under which intra-query learning is
 * activated).
 * 
 * @author immanueltrummer
 *
 */
public class MasterConfig {
	/**
	 * Intra-query learning is activated once query execution
	 * time exceeds this threshold in milliseconds.
	 * Set to 5,000 milliseconds for hybrid version,
	 * otherwise to zero.
	 */
	public static final int learningTimeThreshold = 0;
}
