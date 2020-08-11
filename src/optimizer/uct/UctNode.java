package optimizer.uct;

import joining.BatchedExecutor;
import optimizer.MctsNode;
import query.QueryInfo;

import java.util.*;

import config.JoinConfig;

/**
 * Represents node in UCT search tree.
 *
 * @author immanueltrummer
 */
public class UctNode extends MctsNode {
    /**
     * Used for randomized selection policy.
     */
    final Random random = new Random();
    /**
     * The query for which we are optimizing.
     */
    final QueryInfo query;
    /**
     * Iteration in which node was created.
     */
    final long createdIn;
    /**
     * Level of node in tree (root node has level 0).
     * At the same time the join order index into
     * which table selected in this node is inserted.
     */
    final int treeLevel;
    /**
     * Number of possible actions from this state.
     */
    final int nrActions;
    /**
     * Actions that have not been tried yet - if the
     * heuristic is used, this only contains actions
     * that have not been tried and are recommended.
     */
    final List<Integer> priorityActions;
    /**
     * Assigns each action index to child node.
     */
    final UctNode[] childNodes;
    /**
     * Number of times this node was visited.
     */
    int nrVisits = 0;
    /**
     * Number of times each action was tried out.
     */
    public final int[] nrTries;
    /**
     * Reward accumulated for specific actions.
     */
    public final double[] accumulatedReward;
    /**
     * Total number of tables to join.
     */
    final int nrTables;
    /**
     * Set of already joined tables (each UCT node represents
     * a state in which a subset of tables are joined).
     */
    final Set<Integer> joinedTables;
    /**
     * List of unjoined tables (we use a list instead of a set
     * to enable shuffling during playouts).
     */
    final List<Integer> unjoinedTables;
    /**
     * Associates each action index with a next table to join.
     */
    final int[] nextTable;
    /**
     * Evaluates a given join order and accumulates results.
     */
    final BatchedExecutor executor;
    /**
     * Indicates whether the search space is restricted to
     * join orders that avoid Cartesian products. This
     * flag should only be activated if it is ensured
     * that a given query can be evaluated under that
     * constraint.
     */
    final boolean useHeuristic;
    /**
     * Contains actions that are consistent with the "avoid
     * Cartesian products" heuristic. UCT algorithm will
     * restrict focus on such actions if heuristic flag
     * is activated.
     */
    final Set<Integer> recommendedActions;
    /**
     * Initialize UCT root node.
     *
     * @param roundCtr     current round number
     * @param query        the query which is optimized
     * @param useHeuristic whether to avoid Cartesian products
     * @param executor     evaluates join orders and accumulates query results
     */
    public UctNode(long roundCtr, QueryInfo query, 
    		boolean useHeuristic, BatchedExecutor executor) {
        this.query = query;
        this.nrTables = query.nrJoined;
        createdIn = roundCtr;
        treeLevel = 0;
        nrActions = nrTables;
        priorityActions = new ArrayList<Integer>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            priorityActions.add(actionCtr);
        }
        childNodes = new UctNode[nrActions];
        nrTries = new int[nrActions];
        accumulatedReward = new double[nrActions];
        joinedTables = new HashSet<Integer>();
        unjoinedTables = new ArrayList<>();
        nextTable = new int[nrTables];
        for (int tableCtr = 0; tableCtr < nrTables; ++tableCtr) {
            unjoinedTables.add(tableCtr);
            nextTable[tableCtr] = tableCtr;
        }
        this.executor = executor;
        this.useHeuristic = useHeuristic;
        recommendedActions = new HashSet<Integer>();
        for (int action = 0; action < nrActions; ++action) {
            accumulatedReward[action] = 0;
            recommendedActions.add(action);
        }
    }
    /**
     * Initializes UCT node by expanding parent node.
     *
     * @param roundCtr    current round number
     * @param parent      parent node in UCT tree
     * @param joinedTable new joined table
     */
    public UctNode(long roundCtr, UctNode parent, int joinedTable) {
        createdIn = roundCtr;
        treeLevel = parent.treeLevel + 1;
        nrActions = parent.nrActions - 1;
        childNodes = new UctNode[nrActions];
        nrTries = new int[nrActions];
        accumulatedReward = new double[nrActions];
        query = parent.query;
        nrTables = parent.nrTables;
        joinedTables = new HashSet<Integer>();
        joinedTables.addAll(parent.joinedTables);
        joinedTables.add(joinedTable);
        unjoinedTables = new ArrayList<Integer>();
        unjoinedTables.addAll(parent.unjoinedTables);
        int indexToRemove = unjoinedTables.indexOf(joinedTable);
        unjoinedTables.remove(indexToRemove);
        nextTable = new int[nrActions];
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            accumulatedReward[actionCtr] = 0;
            nextTable[actionCtr] = unjoinedTables.get(actionCtr);
        }
        this.executor = parent.executor;
        // Calculate recommended actions if heuristic is activated
        this.useHeuristic = parent.useHeuristic;
        if (useHeuristic) {
            recommendedActions = new HashSet<Integer>();
            // Iterate over all actions
            for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                // Get table associated with (join) action
                int table = nextTable[actionCtr];
                // Check if at least one predicate connects current
                // tables to new table.
                if (query.connected(joinedTables, table)) {
                    // Add as recommended action and continue
                    recommendedActions.add(actionCtr);
                }
            } // over actions
            if (recommendedActions.isEmpty()) {
                // add all actions to recommended actions
                for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                    recommendedActions.add(actionCtr);
                }
            }
        } // if heuristic is used
        else {
            recommendedActions = null;
        }
        // Collect untried actions, restrict to recommended actions
        // if the heuristic is activated.
        priorityActions = new ArrayList<Integer>();
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            if (!useHeuristic || recommendedActions.contains(actionCtr)) {
                priorityActions.add(actionCtr);
            }
        }
    }
    /**
     * Select most interesting action to try next. Also updates
     * list of unvisited actions.
     *
     * @return index of action to try next
     */
    int selectAction() {
        //System.out.println("untried: " + priorityActions);
        //System.out.println("joinedTables: " + joinedTables);
        // Are there untried actions?
        if (!priorityActions.isEmpty()) {
            int nrUntried = priorityActions.size();
            int actionIndex = random.nextInt(nrUntried);
            int action = priorityActions.get(actionIndex);
            // Remove from untried actions and return
            priorityActions.remove(actionIndex);
            // System.out.println("Untried action: " + action);
            return action;
        } else {
            /*
             * We apply the UCT formula as no actions are untried.
             * We iterate over all actions and calculate their
             * UCT value, updating best action and best UCT value
             * on the way. We start iterations with a randomly
             * selected action to ensure that we pick a random
             * action among the ones with maximal UCT value.
             */
            int offset = random.nextInt(nrActions);
            int bestAction = -1;
            double bestUB = -1;
            for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
                // Calculate index of current action
                int action = (offset + actionCtr) % nrActions;
                // if heuristic is used, choose only from recommended actions
                if (useHeuristic && recommendedActions.size() == 0) {
                    throw new RuntimeException("there are no recommended exception and we are trying to use heuristic");
                }
                if (useHeuristic && !recommendedActions.contains(action))
                    continue;
                // Evaluate UCT formula, balancing exploration and exploitation
                double meanReward = accumulatedReward[action] / nrTries[action];
                double exploration = Math.sqrt(Math.log(nrVisits) / nrTries[action]);
                double UB = meanReward + JoinConfig.explorationFactor * exploration;
                if (UB > bestUB) {
                    bestAction = action;
                    bestUB = UB;
                }
            }
            if (bestAction == -1) {
                throw new RuntimeException("No action selected!");
            }
            return bestAction;
        } // if there are unvisited actions
    }
    /**
     * Returns the action that was most often tried
     * (the first qualifying action if multiple actions
     * have the same number of tries).
     * 
     * @return	index of dominant action
     */
    int dominantAction() {
    	int maxTries = -1;
    	int dominantAction = -1;
    	for (int actionCtr=0; actionCtr<nrActions; ++actionCtr) {
    		if (nrTries[actionCtr] > maxTries) {
    			maxTries = nrTries[actionCtr];
    			dominantAction = actionCtr;
    		}
    	}
    	return dominantAction;
    }
    /**
     * Returns join order that was most often visited.
     * 
     * @return	most visited join order
     */
    public int[] dominantOrder() {
    	int[] order = new int[nrTables];
    	Arrays.fill(order, -1);
    	UctNode node = this;
    	for (int joinCtr=0; joinCtr<nrTables; ++joinCtr) {
    		int dominantAction = node.dominantAction();
    		int dominantTable = node.nextTable[dominantAction];
    		order[joinCtr] = dominantTable;
    		UctNode child = node.childNodes[dominantAction];
    		if (child == null) {
    			node.completeOrderRandom(order);
    			break;
    		} else {
    			node = child;
    		}
    	}
    	return order;
    }
    /**
     * Updates UCT statistics after sampling.
     *
     * @param selectedAction action taken
     * @param reward         reward achieved
     */
    void updateStatistics(int selectedAction, double reward) {
        ++nrVisits;
        ++nrTries[selectedAction];
        accumulatedReward[selectedAction] += reward;
    }
    /**
     * Complete join order starting from current tree level by
     * randomly selecting remaining tables, still considering
     * the no-Cartesian-product heuristic if activated.
     * 
     * @param joinOrder			join order to complete
     */
    void completeOrderRandom(int[] joinOrder) {
        // Last selected table
        int lastTable = joinOrder[treeLevel];
        // Should we avoid Cartesian product joins?
        if (useHeuristic) {
            Set<Integer> newlyJoined = new HashSet<Integer>();
            newlyJoined.addAll(joinedTables);
            newlyJoined.add(lastTable);
            // Iterate over join order positions to fill
            List<Integer> unjoinedTablesShuffled = new ArrayList<Integer>();
            unjoinedTablesShuffled.addAll(unjoinedTables);
            Collections.shuffle(unjoinedTablesShuffled);
            // Iterate over join order decisions not taken yet
            for (int posCtr = treeLevel + 1; posCtr < nrTables; ++posCtr) {
                boolean foundTable = false;
                // Prioritize tables with connecting join predicates
                for (int table : unjoinedTablesShuffled) {
                    if (!newlyJoined.contains(table) &&
                            query.connected(newlyJoined, table)) {
                        joinOrder[posCtr] = table;
                        newlyJoined.add(table);
                        foundTable = true;
                        break;
                    }
                }
                // If no table was found, consider Cartesian products
                if (!foundTable) {
                    for (int table : unjoinedTablesShuffled) {
                        if (!newlyJoined.contains(table)) {
                            joinOrder[posCtr] = table;
                            newlyJoined.add(table);
                            break;
                        }
                    }
                }
            }
       } else {
            // Shuffle remaining tables
            Collections.shuffle(unjoinedTables);
            Iterator<Integer> unjoinedTablesIter = unjoinedTables.iterator();
            // Fill in remaining join order positions
            for (int posCtr = treeLevel + 1; posCtr < nrTables; ++posCtr) {
                int nextTable = unjoinedTablesIter.next();
                while (nextTable == lastTable) {
                    nextTable = unjoinedTablesIter.next();
                }
                joinOrder[posCtr] = nextTable;
            }
        }
    }
    /**
     * Randomly complete join order with remaining tables,
     * invoke evaluation, and return obtained reward.
     *
     * @param joinOrder 	partially completed join order
     * @param timeoutMillis	number of milliseconds until timeout
     * @return obtained reward
     */
    double playout(int[] joinOrder, int timeoutMillis) throws Exception {
    	// Randomly complete join order
    	completeOrderRandom(joinOrder);
        // Evaluate completed join order and return reward
        long startMillis = System.currentTimeMillis();
        boolean success = executor.execute(joinOrder, timeoutMillis);
        long totalMillis = System.currentTimeMillis() - startMillis;
        int firstTable = joinOrder[0];
        double rewardScaling = executor.rewardScaling[firstTable];
        return success?reward(timeoutMillis, totalMillis, rewardScaling):0;
    }
    /**
     * Recursively sample from UCT tree and return reward.
     *
     * @param roundCtr  	current round (used as timestamp for expansion)
     * @param joinOrder 	partially completed join order
     * @param timeoutMillis number of milliseconds until timeout
     * @return achieved reward
     */
    @Override
    public double sample(long roundCtr, int[] joinOrder, 
    		int timeoutMillis) throws Exception {
        // Check if this is a (non-extendible) leaf node
        if (nrActions == 0) {
            // leaf node - evaluate join order and return reward
        	long startMillis = System.currentTimeMillis();
        	boolean success = executor.execute(
        			joinOrder, timeoutMillis);
        	long totalMillis = System.currentTimeMillis() - startMillis;
            int firstTable = joinOrder[0];
            double rewardScaling = executor.rewardScaling[firstTable];
        	return success?reward(timeoutMillis, totalMillis, rewardScaling):0;
        } else {
            // inner node - select next action and expand tree if necessary
            int action = selectAction();
            int table = nextTable[action];
            joinOrder[treeLevel] = table;
            // grow tree if possible
            boolean canExpand = createdIn != roundCtr;
            if (childNodes[action] == null && canExpand) {
                childNodes[action] = new UctNode(roundCtr, this, table);
            }
            // evaluate via recursive invocation or via playout
            UctNode child = childNodes[action];
            double reward = (child != null) ?
                    child.sample(roundCtr, joinOrder, timeoutMillis): 
                    	playout(joinOrder, timeoutMillis);
            // update UCT statistics and return reward
            updateStatistics(action, reward);
            return reward;
        }
    }
    /**
     * Returns reward for a successful join query execution
     * on a data batch with the given timeout. 
     * 
     * @param timeout	number of milliseconds until timeout
     * @param actual	actual execution time in milliseconds
     * @param scaling	reward scaling for given table
     * @return			reward value
     */
    public double reward(int timeout, long actual, double scaling) {
    	//double reward = scaling;
    	double reward = 1.0;
    	//System.out.println("Reward: " + reward);
    	return reward;
    	/*
    	if (actual < 10) {
    		return 1;
    	} else if (actual < 100) {
    		return 0.75;
    	} else if (actual < 1000) {
    		return 0.5;
    	} else if (actual < 10000) {
    		return 0.25;
    	} else {
    		return 0.1;
    	}
    	*/
    	//return 1.0/actual;
    }
}