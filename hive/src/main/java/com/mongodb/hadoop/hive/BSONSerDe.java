package com.mongodb.hadoop.hive;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.Writable;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.*;
import com.mongodb.hadoop.io.BSONWritable;

public class BSONSerDe implements SerDe {
    private static final Log log = LogFactory.getLog(BSONSerDe.class);
    
    private static final int BSON_TYPE = 8;
    private static final String OID = "oid";
    
    private StructTypeInfo docTypeInfo;
    private ObjectInspector docOI;
    public List<String> columnNames;
    public List<String> mongoFields;
    public List<TypeInfo> columnTypes;
    
    private List<Object> row = new ArrayList<Object>();
    
    /**
     * Finds out the information of the table, including the column names and types. 
     */
    @Override
    public void initialize(Configuration conf, Properties tblProps)
	throws SerDeException {
        // regex used to split column names between commas
        String splitCols = "\\s*,\\s*";
        
        // Get the table column names
        String colNamesStr = tblProps.getProperty(serdeConstants.LIST_COLUMNS);
        columnNames = Arrays.asList(colNamesStr.split(splitCols));

        // Get the Mongo collection column mapping
        if (tblProps.containsKey(MongoStorageHandler.MONGO_COLS)) {
            String mongoFieldsStr = tblProps.getProperty(MongoStorageHandler.MONGO_COLS);
            mongoFields = Arrays.asList(mongoFieldsStr.split(splitCols));
        } else {
            mongoFields = columnNames;
        }
        
        if (columnNames.size() != mongoFields.size()) {
            throw new SerDeException("'mongo.columns.mapping' must be of same length as table columns");
        }
        
        // Get the table column types
        String colTypesStr = tblProps.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);
        
        assert( columnNames.size() == columnTypes.size()) :
	    "Column Names and Types don't match in size";
        
        // Get the structure and object inspector
	docTypeInfo = 
            (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
        docOI = 
            TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(docTypeInfo);
    }
    
    /**
     * Given a Writable object of BSON, turn it into a Hive table row
     */
    @Override
    public Object deserialize(Writable writ) throws SerDeException {
        BSONObject doc = null;
        row.clear();
        
        // Make sure it's a BSONWritable object
        if (writ instanceof BSONWritable) {
            doc = ((BSONWritable) writ).getDoc();
        } else {
            throw new SerDeException(getClass().toString() + 
                                     "requires a BSONWritable object, not" + writ.getClass());
        }
        
        // Only lower case names
        BSONObject lower = new BasicBSONObject();
        for (Entry<String, Object> entry : ((BasicBSONObject) doc).entrySet()) {
            if (lower.containsField(entry.getKey().toLowerCase())) {
                log.error("Fields should only be lower cased and not duplicated: " 
                          + entry.getKey());
            } else {
                lower.put(entry.getKey().toLowerCase(), entry.getValue());
            }
        }
        
        // For each field, cast it to a HIVE type and add to the current row
        Object value = null;
        List<String> structFieldNames = docTypeInfo.getAllStructFieldNames();
        for (int i = 0; i < structFieldNames.size(); i++) {
            String fieldName = structFieldNames.get(i);
            try {
                TypeInfo fieldTypeInfo = docTypeInfo.getStructFieldTypeInfo(fieldName);
                value = deserializeField(lower.get(mongoFields.get(i)), fieldTypeInfo);                
            } catch (Exception e) {
                value = null;
            }
            row.add(value);
        }
        
        return row;
    }
    
    /**
     * For a given Object value and its supposed TypeInfo
     * determine and return its Java object representation
     * 
     * Map in here must be of the same type, so instead an embedded doc
     * becomes a struct instead. ***
     */
    public Object deserializeField(Object value, TypeInfo valueTypeInfo) {
        if (value != null) {
	    switch (valueTypeInfo.getCategory()) {
	        case LIST:
		    return deserializeList(value, (ListTypeInfo) valueTypeInfo);
	        case MAP:
		    return deserializeMap(value, (MapTypeInfo) valueTypeInfo);
	        case PRIMITIVE:
		    return deserializePrimitive(value, (PrimitiveTypeInfo) valueTypeInfo);
	        case STRUCT:
		    // Supports both struct and map, but should use struct 
		    return deserializeStruct(value, (StructTypeInfo) valueTypeInfo);
	        case UNION:
		    // Mongo also has no union
		    return null;
	        default:
		    // Must be an unknown (a Mongo specific type)
		    return deserializeMongoType(value);
            }
        }
        return null;
    }
    
    /**
     * Deserialize a List with the same listElemTypeInfo for its elements
     */
    private Object deserializeList(Object value, ListTypeInfo valueTypeInfo) {
        BasicBSONList list = (BasicBSONList) value;
        TypeInfo listElemTypeInfo = valueTypeInfo.getListElementTypeInfo();
	
        for (int i = 0 ; i < list.size() ; i++) {
            list.set(i, deserializeField(list.get(i), listElemTypeInfo));
        }
        return list.toArray();
    }
    
    /**
     * 
     * @param value
     * @param valueTypeInfo
     * @return
     */        
    @SuppressWarnings("unchecked")
    private Object deserializeStruct(Object value, StructTypeInfo valueTypeInfo) {	
        if (value instanceof ObjectId) {
            return deserializeObjectId(value, valueTypeInfo);
        } else {
	    
            Map<Object, Object> map = (Map<Object, Object>) value;
            ArrayList<String> structNames = valueTypeInfo.getAllStructFieldNames();
            ArrayList<TypeInfo> structTypes = valueTypeInfo.getAllStructFieldTypeInfos();
	    
            List<Object> struct = new ArrayList<Object> (structNames.size());
            for (int i = 0 ; i < structNames.size() ; i++) {
                struct.add(deserializeField(map.get(structNames.get(i)), structTypes.get(i)));
            }
            return struct;
        }
    }
    
    /**
     * Also deserialize a Map with the same mapElemTypeInfo
     */
    private Object deserializeMap(Object value, MapTypeInfo valueTypeInfo) {
        BasicBSONObject b = (BasicBSONObject) value;
        TypeInfo mapValueTypeInfo = valueTypeInfo.getMapValueTypeInfo();
        
        for (Entry<String, Object> entry : b.entrySet()) {
            b.put(entry.getKey(), deserializeField(entry.getValue(), mapValueTypeInfo));
        }
	
        return b.toMap();
    }    
    
    /**
     * Most primitives are included, but some are specific to Mongo instances
     */
    private Object deserializePrimitive(Object value, PrimitiveTypeInfo valueTypeInfo) {
        switch (valueTypeInfo.getPrimitiveCategory()) {
	    case BINARY:
		return (byte[]) value;
	    case BOOLEAN:
		return (Boolean) value;
	    case DOUBLE:
		return (Double) value;
	    case FLOAT:
		return (Float) value;
            case INT:
		if (value instanceof Double) {
		    return ((Double) value).intValue(); 
		}
		return (Integer) value;
            case LONG:
		return (Long) value;
            case SHORT:
		return (Short) value;
            case STRING:
		return value.toString();
            case TIMESTAMP:
		if (value instanceof Date) {
		    return new Timestamp(((Date) value).getTime());
		} else if (value instanceof BSONTimestamp) {
		    return new Timestamp(((BSONTimestamp) value).getTime() * 1000L);
		} else {
		    return (Timestamp) value;
		}
            default:
		return deserializeMongoType(value);
        }
    }
    
    
    /**
     * 
     * For Mongo Specific types, return the most appropriate java types
     */
    private Object deserializeMongoType(Object value) {
        // TODO:: Add more here
        if (value instanceof ObjectId) {
            return ((ObjectId) value).toString();
        } else if (value instanceof Symbol) {
            return ((Symbol) value).toString();
        } else {
	    
            log.error("Unable to parse " + value.toString() + " for type " + value.getClass().toString());
            return null;
        }
    }
    
    private Object deserializeObjectId(Object value, StructTypeInfo valueTypeInfo) {
        ArrayList<String> structNames = valueTypeInfo.getAllStructFieldNames();
	
        List<Object> struct = new ArrayList<Object> (structNames.size());
        for (int i = 0 ; i < structNames.size() ; i++) {
            if (structNames.get(i).equals(OID)) {
                struct.add(((ObjectId) value).toString());
            } else if (structNames.get(i).equals("bsonType")) {
                // the bson type is an int order type
                // http://docs.mongodb.org.manual/faq/developers/
                struct.add(BSON_TYPE);
            }
        }
        return struct;
    }
    
    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return docOI;
    }
    
    @Override
    public SerDeStats getSerDeStats() {
        //TODO:: this needs to be determined. what is it?
        return null;
    } 
    
    @Override
    public Class<? extends Writable> getSerializedClass() {
        return BSONWritable.class;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector oi)
	throws SerDeException {
        return new BSONWritable((BSONObject) serializeStruct(obj, (StructObjectInspector) oi, true));
    }
    
    public Object serializeObject(Object obj, ObjectInspector oi) {
        switch (oi.getCategory()) {
            case LIST:
		return serializeList(obj, (ListObjectInspector) oi);
            case MAP:
		return serializeMap(obj, (MapObjectInspector) oi);
            case PRIMITIVE:
		return serializePrimitive(obj, (PrimitiveObjectInspector) oi);
            case STRUCT:
		return serializeStruct(obj, (StructObjectInspector) oi, false);
            case UNION:
            default:
		log.error("Cannot serialize " + obj.toString() + " of type " + obj.toString());
		break;
        }
        return null;
    }
    
    private Object serializeList(Object obj, ListObjectInspector oi) {
        BasicBSONList list = new BasicBSONList();
        List<?> field = oi.getList(obj);
        ObjectInspector elemOI = oi.getListElementObjectInspector();
	
        for (Object elem : field) {
            list.add(serializeObject(elem, elemOI));
        }
	
        return list;
    }
    
    /**
     * Turn struct obj into a BasicBSONObject
     */
    private Object serializeStruct(Object obj, 
				   StructObjectInspector structOI, 
				   boolean isRow) {
        if (!isRow && isObjectIdStruct(structOI)) {
            
            String objectIdString = "";
            for (StructField s : structOI.getAllStructFieldRefs()) {
                if (s.getFieldName().equals(OID)) {
                    objectIdString = structOI.getStructFieldData(obj, s).toString();
                    break;
                }
            }
            return new ObjectId(objectIdString);
        } else {
	    
            BasicBSONObject bsonObject = new BasicBSONObject();
            // fields is the list of all variable names and information within the struct obj
            List<? extends StructField> fields = structOI.getAllStructFieldRefs();
	    
            for (int i = 0 ; i < fields.size() ; i++) {
                StructField field = fields.get(i);
		
                // get corresponding mongoDB field
                String fieldName = isRow ? mongoFields.get(i) : field.getFieldName();
                
                ObjectInspector fieldOI = field.getFieldObjectInspector();
                Object fieldObj = structOI.getStructFieldData(obj, field);
                
                bsonObject.put(fieldName, serializeObject(fieldObj, fieldOI));
            }
	    
            return bsonObject;
        }
    }
    
    /**
     *
     * Given a struct, look to se if it contains the fields that a ObjectId
     * struct should contain
     */
    private boolean isObjectIdStruct(StructObjectInspector structOI) {
        List<? extends StructField> fields = structOI.getAllStructFieldRefs();
	
        // If the struct are of incorrect size, then there's no need to create
        // a list of names
        if (fields.size() != 2) {
            return false;
        }
        ArrayList<String> fieldNames = new ArrayList<String>();
        for (StructField s : fields) {
            fieldNames.add(s.getFieldName());
        }
        return (fieldNames.contains(OID)) && (fieldNames.contains("bsonType"));
    }  
    
    /**
     * For a map of <String, Object> convert to an embedded document 
     */
    private Object serializeMap(Object obj, MapObjectInspector mapOI) {
        BasicBSONObject bsonObject = new BasicBSONObject();
        ObjectInspector mapValOI = mapOI.getMapValueObjectInspector();
	
        // Each value is guaranteed to be of the same type
        for (Entry<?, ?> entry : mapOI.getMap(obj).entrySet()) {        
            String field = entry.getKey().toString();
            Object value = serializeObject(entry.getValue(), mapValOI);
            bsonObject.put(field, value);
        }
        return bsonObject;
    }
    
    /**
     * For primitive types, depending on the primitive type, 
     * cast it to types that Mongo supports
     */
    private Object serializePrimitive(Object obj, PrimitiveObjectInspector oi) {
        switch (oi.getPrimitiveCategory()) {
            case BOOLEAN:
                return (Boolean) obj;
            case BINARY:
            case BYTE:
                return (byte[]) obj;
            case DOUBLE:
            case FLOAT:
                return (Double) obj;
            case LONG:
            case SHORT:
            case INT:
                if (obj instanceof LazyInteger) {
                    return Integer.parseInt(((LazyInteger) obj).toString());
                }
                return (Integer) obj;
            case STRING:
                if (obj instanceof LazyString) {
                    return ((LazyString) obj).toString();
                }
                return (String) obj;
            case TIMESTAMP:
                return new BSONTimestamp(((Long) (((Timestamp) obj).getTime() / 1000L)).intValue(), 1);
            case UNKNOWN:
            case VOID:
            default:
                return null;
        }
    }
}