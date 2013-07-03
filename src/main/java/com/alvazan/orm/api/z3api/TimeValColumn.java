package com.alvazan.orm.api.z3api;

import java.math.BigInteger;

public class TimeValColumn {

	private long time;
	private Object val;
	private Long timestamp;

	public TimeValColumn(BigInteger pk, Object val, Long timestamp) {
		this.time = pk.longValue();
		this.val = val;
		this.timestamp = timestamp;
	}

	public long getTime() {
		return time;
	}

	public Object getVal() {
		return val;
	}

	public Long getTimestamp() {
		return timestamp;
	}

}
