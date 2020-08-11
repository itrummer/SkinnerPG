package optimizer.brue;

import org.apache.commons.lang3.mutable.MutableBoolean;
import query.QueryInfo;
import statistics.JoinStats;

import java.lang.reflect.Array;
import java.sql.SQLOutput;
import java.util.*;

import config.JoinConfig;
import joining.BatchedExecutor;
import optimizer.MctsNode;
import optimizer.uct.UctNode;

/**
 * Represents node in brue search tree.
 *
 * @author Junxiong Wang
 */
public class BrueNode {
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
    public final int treeLevel;
    /**
     * Number of possible actions from this state.
     */
    public final int nrActions;
    /**
     * Actions that have not been tried yet - if the
     * heuristic is used, this only contains actions
     * that have not been tried and are recommended.
     */
    final List<Integer> priorityActions;
    /**
     * Assigns each action index to child node.
     */
    public final BrueNode[] childNodes;
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
    public final int[] nextTable;
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

    static HashMap<JoinOrder, BrueNode> nodeMap = new HashMap<>();

    /**
     * Initialize UCT root node.
     *
     * @param roundCtr     	current round number
     * @param query        	the query which is optimized
     * @param useHeuristic 	whether to avoid Cartesian products
     * @param executor		executes selected join order on batches
     */
    public BrueNode(long roundCtr, QueryInfo query,
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
        childNodes = new BrueNode[nrActions];
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
    public BrueNode(long roundCtr, BrueNode parent, int joinedTable) {
        createdIn = roundCtr;
        treeLevel = parent.treeLevel + 1;
        nrActions = parent.nrActions - 1;
        childNodes = new BrueNode[nrActions];
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
                    recommendedActions.add(actionCtr);
                } // over predicates
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
    /**
     * Recursively sample from UCT tree and return reward.
     *
     * @param roundCtr  current round (used as timestamp for expansion)
     * @param joinOrder partially completed join order
     * @return achieved reward
     */
    public double sample(long roundCtr, int[] joinOrder, 
                         int selectSwitch, boolean expand, 
                         MutableBoolean restart, 
                         int timeoutMillis) throws Exception {
        //System.out.println("roundCtr:" + roundCtr);
        //System.out.println("selectSwitchFun:" + selectSwitchFun);
        //System.out.println("order " + Arrays.toString(joinOrder));
        if (treeLevel == nrTables) {
//            System.out.println("order " + Arrays.toString(joinOrder));
            // leaf node - evaluate join order and return reward
        	long startMillis = System.currentTimeMillis();
        	boolean success = executor.execute(
        			joinOrder, timeoutMillis);
        	long totalMillis = System.currentTimeMillis() - startMillis;
            int firstTable = joinOrder[0];
            double rewardScaling = executor.rewardScaling[firstTable];
        	return success?reward(timeoutMillis, totalMillis, rewardScaling):0;
        }
        //pick up action for the next step
        int action = 0;
        if (treeLevel < selectSwitch) {
            //explore the current best action
            //System.out.println("explore new order=====");
            action = explorationPolicy();
            //System.out.println("explore:" + action);
        } else {
            //select the new action
            //System.out.println("exploit best order=====");
            action = estimationPolicy();
            //System.out.println("random:" + action);
        }

        int table = nextTable[action];
        joinOrder[treeLevel] = table;
        //System.out.println("table:" + table);
        double reward = 0;
        if (childNodes[action] != null) {
            //go to the lower level
            reward = childNodes[action].sample(
            		roundCtr, joinOrder, selectSwitch, 
            		false, restart, timeoutMillis);
        } else {
            //go the the lower level
            JoinOrder currentOrder = new JoinOrder(Arrays.copyOfRange(joinOrder, 0, treeLevel + 1));
            BrueNode nextNode;
            if (nodeMap.containsKey(currentOrder)) {
                nextNode = nodeMap.get(currentOrder);
            } else {
                nextNode = new BrueNode(roundCtr, this, table);
                nodeMap.put(currentOrder, nextNode);
            }
            if (expand) {
                //Expand the BRUE tree
                childNodes[action] = nextNode;
                if (treeLevel != selectSwitch) {
                    restart.setTrue();
                }
                //only expand one time.
                expand = false;
            }
            reward = nextNode.sample(roundCtr, joinOrder, 
            		selectSwitch, expand, restart, timeoutMillis);
        }
        if (treeLevel == selectSwitch) {
            //System.out.println("update reward");
            updateStatistics(action, reward);
        }
        return reward;
    }

    private int estimationPolicy() {
        int offset = random.nextInt(nrActions);
        int bestAction = -1;
        double bestQuality = -1;
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            // Calculate index of current action
            int action = (offset + actionCtr) % nrActions;
            if (useHeuristic && !recommendedActions.contains(action))
                continue;
            double meanReward = (nrTries[action] > 0) ? accumulatedReward[action] / nrTries[action] : 0;
            //System.out.println("action:" + action);
            //System.out.println("meanReward:" + meanReward);
            if (meanReward > bestQuality) {
                bestAction = action;
                bestQuality = meanReward;
            }
        }
        return bestAction;
    }

    private int explorationPolicy() {
        int offset = random.nextInt(nrActions);
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            int action = (offset + actionCtr) % nrActions;
            if (useHeuristic && !recommendedActions.contains(action))
                continue;
            return action;
        }
        return offset;
    }

//    public boolean getOptimalPolicy(int[] joinOrder, int roundCtr) {
////        for (int i = 0; i < nrActions; i++) {
////            System.out.println("reward:" + accumulatedReward[i]);
////        }
//
//        if (treeLevel < nrTables) {
//            int action = estimationPolicy();
//            int table = nextTable[action];
//            joinOrder[treeLevel] = table;
//            if (childNodes[action] != null)
//                return childNodes[action].getOptimalPolicy(joinOrder, roundCtr);
//            else {
//                childNodes[action] = new BrueNode2(roundCtr, this, table);
//                BrueNode2 child = childNodes[action];
//                child.getOptimalPolicy(joinOrder, roundCtr);
//                return false;
//            }
//        }
//
//        //System.out.println(Arrays.toString(joinOrder));
//        return true;
//    }
/*
    public void executePhaseWithBudget(int[] joinOrder) throws Exception {
        joinOp.execute(joinOrder);
    }
*/
    
    /**
     * Returns join order that was most often visited.
     * 
     * @return	most visited join order
     */
    public int[] dominantOrder() {
    	int[] order = new int[nrTables];
    	Arrays.fill(order, -1);
    	BrueNode node = this;
    	for (int joinCtr=0; joinCtr<nrTables; ++joinCtr) {
    		int dominantAction = node.dominantAction();
    		int dominantTable = node.nextTable[dominantAction];
    		order[joinCtr] = dominantTable;
    		BrueNode child = node.childNodes[dominantAction];
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
     * Returns the action with highest average reward.
     * 
     * @return	index of dominant action
     */
    int dominantAction() {
    	double maxReward = -1;
    	int dominantAction = -1;
    	for (int actionCtr=0; actionCtr<nrActions; ++actionCtr) {
    		double avgReward = nrTries[actionCtr]==0?0:
    			accumulatedReward[actionCtr]/nrTries[actionCtr];
    		if (avgReward > maxReward) {
    			maxReward = avgReward;
    			dominantAction = actionCtr;
    		}
    	}
    	return dominantAction;
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
    
    public void clearNodeMap() {
        nodeMap.clear();
    }
}