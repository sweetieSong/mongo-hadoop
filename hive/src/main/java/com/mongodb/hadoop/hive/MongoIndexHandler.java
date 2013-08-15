package com.mongodb.hadoop.hive;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.index.AbstractIndexHandler;
import org.apache.hadoop.hive.ql.index.HiveIndexQueryContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

import com.mongodb.util.JSON;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.MongoURI;
import com.mongodb.hadoop.util.MongoConfigUtil;
/**
 * The MongIndexHandler will create a MongoDB index for every index
 * created in Hive
 */

public class MongoIndexHandler extends AbstractIndexHandler {

    private Configuration configuration;
    private static final Log LOG = LogFactory.getLog(MongoIndexHandler.class);
    public static final String INDEX_ORDER = "indexOrder";
    
    @Override
    public void analyzeIndexDefinition(Table baseTable, 
            Index index, Table indexTable)
            throws HiveException {
        Map<String, String> tblParams = baseTable.getParameters();
        if (!tblParams.containsKey(MongoStorageHandler.MONGO_URI)) {
            throw new HiveException("You must specify a 'mongo.uri' in TBLPROPERTIES");
        }
        String mongoURI = tblParams.get(MongoStorageHandler.MONGO_URI);

        // Each Table must have columns to begin with 
        // See the native index handlers for this
        StorageDescriptor storageDesc = index.getSd();
        if (this.usesIndexTable() && indexTable != null) {
            StorageDescriptor indexTableSd = storageDesc.deepCopy();
            List<FieldSchema> indexTblCols = indexTableSd.getCols();
            FieldSchema bucketFileName = new FieldSchema("_bucketname", "string", "");
            indexTblCols.add(bucketFileName);
            FieldSchema offSets = new FieldSchema("_offset", "bigint", "");
            indexTblCols.add(offSets);
            indexTable.setSd(indexTableSd);
        }

        // Pass the MongoDB indices created here to the index table to 
        // drop in Mongodb when the Hive Table is dropped 
        String indexOrder = createMongoIndex(baseTable, index);
        Map<String, String> idxParams = indexTable.getParameters();
        idxParams.put(INDEX_ORDER, indexOrder);

        // Also need to pass in the MONGO_URI of the basetable, 
        // else the index table doesn't know where to connect to
        idxParams.put(MongoStorageHandler.MONGO_URI, mongoURI);

        // We can also specify the StorageHandler for the table here
        idxParams.put(META_TABLE_STORAGE
                , "com.mongodb.hadoop.hive.MongoIndexStorageHandler");
        indexTable.setParameters(idxParams);
        
        // Cannot drop index tables, need to set the TableType to something else 
        // NOTE:: dropping an index does not trigger the Hooks
        indexTable.setTableType(TableType.MANAGED_TABLE.toString());
    }

    /**
     * MongoDB indices are updates automatically, there's no need for 
     * deferred rebuilds
     */
    @Override
    public List<Task<?>> generateIndexBuildTaskList(
            org.apache.hadoop.hive.ql.metadata.Table baseTable, 
            Index index,
            List<Partition> indexTblPartitions, 
            List<Partition> baseTblPartitions,
            org.apache.hadoop.hive.ql.metadata.Table indexTable,
            Set<ReadEntity> inputs, 
            Set<WriteEntity> outputs) throws HiveException {

        return new ArrayList<Task<?>>();
    }

    @Override
    public void generateIndexQuery(List<Index> indexes, 
            ExprNodeDesc predicate,
            ParseContext pctx, 
            HiveIndexQueryContext queryContext) {
    }

    @Override
    public boolean usesIndexTable() {
        return true;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    @Override
    public void setConf(Configuration conf) {
        configuration = conf;
    }

    /**
     * Given the index and baseTable, look to the MongoDB URI location to create
     * the corresponding index in MongoDB as well, return the serialized DBObject
     * that contains all the DBObjects used in ensureIndex as values.
     */
    private String createMongoIndex(Table baseTable, Index index) throws HiveException {

        // Get the collection from the MongoURI
        Map<String, String> tblParams = baseTable.getParameters();
        String mongoURIStr = tblParams.get(MongoStorageHandler.MONGO_URI);
        DBCollection coll = MongoConfigUtil.getCollection(new MongoURI(mongoURIStr));

        BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        Map<String, String> idxParams = index.getParameters();

        BasicDBObject indexOrder = new BasicDBObject();
        
        // Create a DBObject of each field and its order. Either 1 for ascending
        // or -1 for descending. This DBObject is used for ensureIndex 
        for (FieldSchema schema : index.getSd().getCols()) {
            String name = schema.getName();
            String nameKey = name + ".order";
            BasicDBObject mongoIndex = new BasicDBObject();

            Integer order = 1;
            // If there's a specified order, use it, else default to 1
            if (idxParams.containsKey(nameKey)) {
                order = Integer.parseInt(idxParams.get(nameKey));
            } 
            mongoIndex.put(name, order);
            // Create the Index
            coll.createIndex(mongoIndex);
            // Also put the DBObject into indexOrder, which will be passed back
            indexOrder.put(name, mongoIndex);
        }
        // Table Parameters is Map<String,String>, so return the MongoDB serialized
        // DBObject
        return JSON.serialize(indexOrder);
    }

}
