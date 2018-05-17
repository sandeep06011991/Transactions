package edu.umass.cs.transaction.exceptions;

import java.util.HashMap;

/**
 * @author arun
 * 
 *         Response codes for transaction operations.
 *         Difference between TxState and ResponseCode
 *         TxState is more internal
 *         ResponseCode is used to infer response at the issueing client
 */
public enum ResponseCode {
	/**
	 * Indicates that a lock acquisition attempt failed.
	 * Lock is held by another transaction
	 */
	LOCK_FAILURE(11),

	TIMEOUT(17),
	/*Some operation timed out*/
	/**
	 * Indicates that a lock release attempt failed.
	 * 
	 */
	UNLOCK_FAILURE(12),

	/**
	 * 
	 */
	IOEXCEPTION(13),

	/**
	 * Indicates that an individual transaction operation failed.
	 */
	TXOP_FAILURE(14),

	/**
	 * A commit failed either because of IO or other failures or because the
	 * transaction was already aborted. Attempting either a commit (again) or an
	 * abort in response to a commit failure is safe. If the commit failed
	 * because the transaction was already aborted, it is best to do nothing.
	 */
	COMMIT_FAILURE(15),

	/**
	 * An abort failed either because of IO or other failures or because the
	 * transaction was already committed. Attempting either a commit (again) or
	 * an abort in response to a commit failure is safe. If the abort failed
	 * because the transaction was already committed, it is best to do nothing.
	 */
	ABORT_FAILURE(16),

	COMMITTED(18),

	COMPLETE(19),

	TARGET(20),

	OVERLOAD(21),

	RECOVERING(22),
	;

	final int code;

	ResponseCode(int code) {
		this.code = code;
	}

	public int getInt(){return code;}

	static HashMap<Integer,ResponseCode> map = new HashMap<>();

	static{
		for(ResponseCode rpe:ResponseCode.values()){
			map.put(new Integer(rpe.getInt()),rpe);
		}
	}

	public static ResponseCode getResponseCodeFromInt(int a){
		return map.get(new Integer(a));
	}
}
