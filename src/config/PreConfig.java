package config;

/**
 * Parameters that influence primarily the pre-processing phase.
 * 
 * @author immanueltrummer
 *
 */
public class PreConfig {
	/**
	 * Whether to create temporary tables for each base
	 * table which allows us to apply unary predicates
	 * and projections. It also allows us to delete
	 * already processed tuples. On the other side,
	 * creating intermediate tables creates significant
	 * overheads during pre-processing.
	 */
	//public static final boolean createTempTables = false;
	/**
	 * Describes pre-processing mode, in particular which
	 * tables are copied.
	 */
	public static final PreCopyMode preCopyMode = PreCopyMode.COPY_NONE;
}
