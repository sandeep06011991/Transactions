package edu.umass.cs.transaction.txpackets;

import edu.umass.cs.transaction.protocol.TxSecondaryProtocolTask;

public enum TxState {
    INIT,

    ABORTED,

    COMPLETE,

    COMMITTED;


    public static int getIntFromTxState(TxState txState){
        switch(txState) {
            case INIT:
                return 0;
            case ABORTED:
                return 1;
            case COMMITTED:
                return 2;
            case COMPLETE:
                return 3;
        }
        return -1;
    }

    public static TxState getTxStateFromInt(int txNo){
        switch(txNo){
            case 0:
                return INIT;
            case 1:return ABORTED;
            case 2:return COMMITTED;
            case 3:return COMPLETE;
            default:return null;
        }
    }
}
