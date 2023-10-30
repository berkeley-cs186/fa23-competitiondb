package edu.berkeley.cs186.database.contest;

import edu.berkeley.cs186.database.memory.EvictionPolicy;
import edu.berkeley.cs186.database.memory.LRUEvictionPolicy;

public class ContestSetup {

    // Select your buffer eviction policy!
    public static final EvictionPolicy EVICTION_POLICY = new LRUEvictionPolicy();

    public static final String[][] INDICES_TO_BUILD = {
            // Format ("table", "column")
            // Examples:
            // {"customer", "c_custkey"},
            // {"part", "p_partkey"},
    };
}
