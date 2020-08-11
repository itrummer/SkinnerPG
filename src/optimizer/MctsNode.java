package optimizer;

/**
 * Represents a node in the search tree of one of
 * the Monte-Carlo Tree Search methods.
 * 
 * @author immanueltrummer
 *
 */
public abstract class MctsNode {
    /**
     * Recursively sample from MCTS tree and return reward.
     *
     * @param roundCtr  	current round
     * @param joinOrder 	partially completed join order
     * @param timeoutMillis number of milliseconds until timeout
     * @return achieved reward
     */
    public abstract double sample(long roundCtr, 
    		int[] joinOrder, int timeoutMillis) throws Exception;
}
