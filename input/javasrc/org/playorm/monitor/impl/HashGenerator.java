package org.playorm.monitor.impl;

public interface HashGenerator {

	/**
	 * Returns the node number this should run on
	 * 
	 * @param monitorHash
	 * @param numUpWebNodes
	 * @return
	 */
	int generate(int monitorHash, int numUpWebNodes);

}
