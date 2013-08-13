package com.mongodb.hadoop.hive;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.index.AbstractIndexHandler;
import org.apache.hadoop.hive.ql.index.HiveIndexQueryContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

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
    private static final Log LOG = LogFactory.getLog(
            MongoIndexHandler.class.getName());
    
    @Override
    public void analyzeIndexDefinition(Table baseTable, 
            Index index, Table indexTable)
            throws HiveException {
        Map<String, String> tblParams = baseTable.getParameters();
        if (!tblParams.containsKey(MongoStorageHandler.MONGO_URI)) {
            throw new HiveException("You must specify a 'mongo.uri' in TBLPROPERTIES");
        }
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

        createMongoIndex(baseTable, index);
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
     * the corresponding index in MongoDB as well
     */
    private void createMongoIndex(Table baseTable, Index index) throws HiveException {

        Map<String, String> tblParams = baseTable.getParameters();
        String mongoURIStr = tblParams.get(MongoStorageHandler.MONGO_URI);
        DBCollection coll = MongoConfigUtil.getCollection(new MongoURI(mongoURIStr));

        // Create a DBObject of each field and its order. Either 1 for ascending
        // or -1 for descending. A BasicDBObjectBuilder preserves the order
        BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        Map<String, String> idxParams = index.getParameters();
        for (FieldSchema schema : index.getSd().getCols()) {
            String name = schema.getName();
            String order = name + ".order";
            BasicDBObject mongoIndex = new BasicDBObject();

            if (idxParams.containsKey(order)) {
                mongoIndex.put(name, Integer.parseInt(idxParams.get(order)));
            } else {
                mongoIndex.put(name, 1);
            }
            coll.createIndex(mongoIndex);
        }
    }

}
