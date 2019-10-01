package com.natebean.models;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@SuppressWarnings("DefaultAnnotationParam") // being explicit for the example
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_t")
@JsonSubTypes({ @JsonSubTypes.Type(value = ProductionLog.class, name = "pl"),
                @JsonSubTypes.Type(value = GapLog.class, name = "gl"),
                @JsonSubTypes.Type(value = GapLogProductionLogSplitRecord.class, name = "glpl") })
public interface JSONSerdeCompatible {

}