package com.mongodb.hadoop.hive;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.OutputFormat;

import com.mongodb.DBCollection;
import com.mongodb.MongoURI;
import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.util.JSON;

/*
 * Used to sync documents in some MongoDB collection with
 * rows in a Hive table
 */
public class MongoIndexStorageHandler extends DefaultStorageHandler {

    private static final Log LOG = LogFactory.getLog(MongoIndexStorageHandler.class);

    public MongoIndexStorageHandler() { }

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return TextInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return HiveIgnoreKeyTextOutputFormat.class;
    }
    
    @Override
    public HiveMetaHook getMetaHook() {
        return new MongoIndexHiveMetaHook();
    }
       /*
     * HiveMetaHook used to define events triggered when a hive table is
     * created and when a hive table is dropped.
     */
    private class MongoIndexHiveMetaHook implements HiveMetaHook {
        @Override
        public void preCreateTable(Table tbl) throws MetaException {
        }
        
        @Override
        public void commitCreateTable(Table tbl) throws MetaException {
        }
        
        @Override
        public void rollbackCreateTable(Table tbl) throws MetaException {
        }
        
        @Override
        public void preDropTable(Table tbl) throws MetaException {
            // TODO:: This should be changed to commitDropTable, 
            // but now it's a show of how it's at least dropping the tables
            boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
            
            if (!isExternal) {
                Map<String, String> tblParams = tbl.getParameters();

                // Need to contain the MongoURI to drop the indices in Mongo
                if (tblParams.containsKey(MongoStorageHandler.MONGO_URI)) {
                    String mongoURIStr = tblParams.get(MongoStorageHandler.MONGO_URI);
                    DBCollection coll = MongoConfigUtil.getCollection(new MongoURI(mongoURIStr));

                    if (tblParams.containsKey(MongoIndexHandler.INDEX_ORDER)) {
                        // Get the String representation and parse it into a 
                        // MongoDB Object
                        String stringIndices = tblParams.get(MongoIndexHandler.INDEX_ORDER);
                        BasicDBObject indexOrder = (BasicDBObject) JSON.parse(stringIndices);

                        // For every DBObject that's used in ensureIndex, 
                        // drop the same Object
                        for (Object i : indexOrder.values()) {
                            coll.dropIndex((BasicDBObject) i);
                        }
                    } else {
                        throw new MetaException("No 'indexOrder' property found. Indices not dropped.");
                    }
                } else {
                    throw new MetaException("No 'mongo.uri' property found. Indices not dropped.");
                }
            }        }

        @Override
        public void commitDropTable(Table tbl, boolean deleteData)
                throws MetaException {
            
        }
        
        @Override
        public void rollbackDropTable(Table tbl) throws MetaException {
        }
    }
}
