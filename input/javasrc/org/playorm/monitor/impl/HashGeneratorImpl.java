package org.playorm.monitor.impl;

public class HashGeneratorImpl implements HashGenerator {

	@Override
	public int generate(int monitorHash, int numUpWebNodes) {
		return monitorHash % numUpWebNodes;
	}

}
