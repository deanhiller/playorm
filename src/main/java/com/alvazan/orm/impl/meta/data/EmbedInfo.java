package com.alvazan.orm.impl.meta.data;

import com.alvazan.orm.api.z8spi.conv.Converter;
//This is just a DTO to wire few parameters for MetaEmbeddedSimple.java
public class EmbedInfo {

    private Converter converter;
    private Converter valConverter;
    private Class<?> type;
    private Class<?> valueType;

    public Converter getConverter() {
        return this.converter;
    }

    public void setConverter(Converter converter) {
        this.converter = converter;
    }

    public Converter getValConverter() {
        return this.valConverter;
    }

    public void setValConverter(Converter valConverter) {
        this.valConverter = valConverter;
    }

    public Class<?> getType() {
        return this.type;
    }

    public void setType(Class<?> type) {
        this.type = type;
    }

    public Class<?> getValueType() {
        return this.valueType;
    }

    public void setValueType(Class<?> valueType) {
        this.valueType = valueType;
    }

}
