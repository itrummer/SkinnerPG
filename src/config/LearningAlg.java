package config;

/**
 * The reinforcement learning algorithm to
 * use for join order learning.
 * 
 * @author immanueltrummer
 *
 */
public enum LearningAlg {
	UCT, 		// select join orders via UCT 
	BRUE, 		// select join orders via BRUE
	PRE_PG_OPT	// select join orders via PG optimizer
				// after executing pre-processing step
}
