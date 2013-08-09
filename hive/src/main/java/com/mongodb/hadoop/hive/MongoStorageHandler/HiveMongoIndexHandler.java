package com.mongodb.hadoop.hive;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoURI;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class HiveMongoIndexHandler extends AbstractIndexHandler {

    private Configuration configuration;
    private static final Log LOG = LogFactory.getLog(
            HiveMongoIndexHandler.class.getName());
    
    @Override
    public void analyzeIndexDefinition(Table baseTable, 
            Index index, Table indexTable)
            throws HiveException {
        Map<String, String> tblParams = baseTable.getParameters();
        if (!tblParams.containsKey(MongoStorageHandler.MONGO_URI)) {
            throw new HiveException("You must specify a 'mongo.uri' in TBLPROPERTIES");
        }
    }

    @Override
    public List<Task<?>> generateIndexBuildTaskList(
            org.apache.hadoop.hive.ql.metadata.Table baseTable, 
            Index index,
            List<Partition> indexTblPartitions, 
            List<Partition> baseTblPartitions,
            org.apache.hadoop.hive.ql.metadata.Table indexTable,
            Set<ReadEntity> inputs, 
            Set<WriteEntity> outputs) throws HiveException {

        Map<String, String> tblParams = baseTable.getParameters();
        String mongoURIStr = tblParams.get(MongoStorageHandler.MONGO_URI);
        DBCollection coll = MongoConfigUtil.getCollection(new MongoURI(mongoURIStr));
        
        for (FieldSchema schema : index.getSd().getCols()) {
            coll.createIndex(new BasicDBObject(schema.getName(), 1));
        }
        
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
        return false;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    @Override
    public void setConf(Configuration conf) {
        configuration = conf;
    }

}
