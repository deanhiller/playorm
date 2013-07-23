package com.alvazan.orm.impl.meta.data.collections;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.alvazan.orm.api.z8spi.conv.Converter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class SimpleSet<T> extends SimpleAbstractCollection<T> implements Set<T> {

	@SuppressWarnings("unused")
	private static final long serialVersionUID = 1L;

	public SimpleSet(Converter converter, List<byte[]> keys) {
		super(converter, keys);
	}

    public SimpleSet(List<T> keys) {
        super(keys);
    }

	public Object clone() throws CloneNotSupportedException {
        SimpleSet v = (SimpleSet) super.clone();
        SimpleSet[] current = this.toArray(new SimpleSet[0]);
        SimpleSet[] clone = Arrays.copyOf(current, this.size());
        List<SimpleSet> asList = Arrays.asList(clone);
        v.addAll(asList);

        return v;
    }
}
