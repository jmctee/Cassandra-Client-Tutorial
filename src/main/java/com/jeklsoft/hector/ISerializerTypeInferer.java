package com.jeklsoft.hector;

import me.prettyprint.hector.api.Serializer;

public interface ISerializerTypeInferer {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T> Serializer<T> getSerializer(Object value);

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T> Serializer<T> getSerializer(Class<?> valueClass);
}
