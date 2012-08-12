package com.alvazan.orm.api.spi2;

import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;

@NoSqlDiscriminatorColumn(value="id")
public class DboColumnIdMeta extends DboColumnCommonMeta {

	@Override
	public void translateToColumn(InfoForIndex<TypedRow> info) {
		TypedRow entity = info.getEntity();
		RowToPersist row = info.getRow();

		Object id = entity.getRowKey();
		if(id == null)
			throw new IllegalArgumentException("rowKey cannot be null");
		
		byte[] byteVal = convertToStorage2(id);
		row.setKey(byteVal);
		
		StorageTypeEnum storageType = getStorageType();
		addIndexInfo(info, id, byteVal, storageType);
		//NOTICE: there is no call to remove because if an id is changed, it is a new entity and we only need to add index not remove since
		//the old entity was not deleted during this save....we remove from index on remove of old entity only
	}

	
}
