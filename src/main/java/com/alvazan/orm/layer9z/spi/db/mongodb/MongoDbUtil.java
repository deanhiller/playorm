package com.alvazan.orm.layer9z.spi.db.mongodb;

import java.util.Set;

import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.mongodb.DBObject;

public class MongoDbUtil {
	static void processColumns(DBObject mdbRow, Row r) {
		Set<String> columns = mdbRow.keySet();
		for (String col : columns) {
			if(!col.equalsIgnoreCase("_id")) {
				byte[] name = StandardConverters.convertToBytes(col);
				byte[] val = StandardConverters.convertToBytes(mdbRow.get(col));
				Column c = new Column();
				c.setName(name);
				if (val.length != 0)
					c.setValue(val);
				r.put(c);
			}
		}
	}

}
