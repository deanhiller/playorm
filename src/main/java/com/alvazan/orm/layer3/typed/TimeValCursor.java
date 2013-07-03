package com.alvazan.orm.layer3.typed;

import java.math.BigInteger;

import com.alvazan.orm.api.z3api.TimeValColumn;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;

public class TimeValCursor<S> extends AbstractCursor<TimeValColumn> {

	private AbstractCursor<Column> curs;
	private DboColumnIdMeta idColumnMeta;
	private DboColumnMeta colMeta;

	public TimeValCursor(DboTableMeta metaClass, AbstractCursor<Column> curs) {
		idColumnMeta = metaClass.getIdColumnMeta();
		colMeta = metaClass.getAllColumns().iterator().next();
		this.curs = curs;
	}

	@Override
	public void beforeFirst() {
		curs.beforeFirst();
	}

	@Override
	public void afterLast() {
		curs.afterLast();
	}

	@Override
	public Holder<TimeValColumn> nextImpl() {
		Holder<Column> nextImpl = curs.nextImpl();
		return translate(nextImpl);
	}

	@Override
	public Holder<TimeValColumn> previousImpl() {
		Holder<Column> previous = curs.previousImpl();
		return translate(previous);
	}

	private Holder<TimeValColumn> translate(Holder<Column> item) {
		if(item == null)
			return null;
		Column col = item.getValue();
		BigInteger pk = (BigInteger) idColumnMeta.convertFromStorage2(col.getName());
		Object val = colMeta.convertFromStorage2(col.getValue());
		TimeValColumn tv = new TimeValColumn(pk, val, col.getTimestamp());
		return new Holder<TimeValColumn>(tv);
	}

}
