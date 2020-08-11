package config;

/**
 * Represents different modes for pre-processing.
 * 
 * @author immanueltrummer
 *
 */
public enum PreCopyMode {
	COPY_ALL, // copy all input tables (allows to apply unary predicates and to delete tuples)
	COPY_CONSTRAINED,	// copy only input tables with unary predicates
	COPY_NONE			// do not copy any input tables
}
