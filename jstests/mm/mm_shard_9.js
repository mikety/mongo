/*
*  * Test that createCollection properly updates the metadata for an unsharded collection.
*   */

 TestData.skipCheckingUUIDsConsistentAcrossCluster = true;
 (function() {
     'use strict';
     
     const st = new ShardingTest({shards: 3, mongos: 1});
     const mongos = st.s0;
     const dbName = "test_create_collection";
     
     const collName = "unsharded";
     const namespace = dbName + "." + collName;
     jsTestLog("create global collection");
     assert.commandWorked(mongos.getDB(dbName).runCommand({create: collName, global: true, sharded: false}));
     
     jsTestLog("check config metadata");
     assert.neq(null, mongos.getDB("config").databases.findOne({_id: dbName}));
     assert.neq(null, 
                mongos.getDB("config").collections.findOne( 
                    {_id: namespace, partitioned: false, global: true, key: {_id: 1}}));

     assert.neq(null, mongos.getDB("config").chunks.findOne({ns: namespace}));
     
     jsTestLog("flush routing table on all shards");
     st.shard0.getDB("admin").runCommand({_flushRoutingTableCacheUpdates: namespace});
     st.shard1.getDB("admin").runCommand({_flushRoutingTableCacheUpdates: namespace});
     st.shard2.getDB("admin").runCommand({_flushRoutingTableCacheUpdates: namespace});
     
     jsTestLog("check catalog cache on shards");
     assert.neq(null, 
                st.shard0.getDB("config").cache.collections.findOne( 
                    {_id: namespace, partitioned: false, global: true, key: {_id: 1}}));
     assert.neq(null, 
                st.shard1.getDB("config").cache.collections.findOne( 
                    {_id: namespace, partitioned: false, global: true, key: {_id: 1}}));
     assert.neq(null, 
                st.shard2.getDB("config").cache.collections.findOne(
                    {_id: namespace, partitioned: false, global: true, key: {_id: 1}}));
     
     assert.eq(1, 
               st.shard0.getDB("config")
                 .getCollection("cache.chunks.test_create_collection.unsharded")
                 .find()
                 .toArray()
                 .length);
     assert.eq(1, 
               st.shard1.getDB("config")
                 .getCollection("cache.chunks.test_create_collection.unsharded")
                 .find()
                 .toArray()
                 .length);
     assert.eq(1, 
               st.shard2.getDB("config")
                 .getCollection("cache.chunks.test_create_collection.unsharded")
                 .find()
                 .toArray()
                 .length);
     
     st.stop();
})();
