package com.alvazan.orm.impl.meta.data;

public class Tuple<D> {
	private D proxy;
	private Object entityId;
	public D getProxy() {
		return proxy;
	}
	public void setProxy(D proxy) {
		this.proxy = proxy;
	}
	public Object getEntityId() {
		return entityId;
	}
	public void setEntityId(Object entityId) {
		this.entityId = entityId;
	}
}