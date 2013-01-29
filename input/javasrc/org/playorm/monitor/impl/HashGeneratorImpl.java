package org.playorm.monitor.impl;

public class HashGeneratorImpl implements HashGenerator {

	@Override
	public int generate(int monitorHash, int numUpWebNodes) {
		//java's module returns negative numbers if hash is negative here.
		return Math.abs(monitorHash) % numUpWebNodes;
	}

}
