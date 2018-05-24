package edu.umass.cs.transaction;

import java.util.Properties;

public final class TxConstants {

    /*There are 2 modes in which coordinators are elected
    *   1) Where a fixed coordinator group is chosen as coordinator
    *   2) Where a participant group is chosen as coordinator */

    public static final boolean fixedGroups=false;

    public static final int noFixedGroups = 3;

    public static final String coordGroupState = "1";

    public static final String coordGroupPrefix = "fixedGroup";

    public static final int MAX_CONCURRENT_TXN = 30;
}
