/**
 * Test multi-master repl to mongog.
 */
(function() {
    "use strict";

    load("jstests/libs/write_concern_util.js");  // for [stop|restart]ServerReplication.

    const replTestSource = new ReplSetTest({
        name: "source_rs",
        nodes: [{}, {rsConfig: {priority: 0}}],
        nodeOptions: {enableMajorityReadConcern: 'true',
                      binVersion: "last-stable"}
    });
    replTestSource.startSet();
    replTestSource.initiate();

    const dbName = "mm_replication_mongog";
    const collName = "MultiMasterCollection";

    let primary = replTestSource.getPrimary();
    let secondary = replTestSource.getSecondary();
    let primaryDB = primary.getDB(dbName);
    let primaryColl = primaryDB[collName];

    jsTestLog(" started source replicaset");

    const mongog = new ReplSetTest({
        name: "mongog_rs",
        nodes: [
            {setParameter: {
               isMongoG: true,
               testSourceRSPort: replTestSource.getPrimary().port}}],
        nodeOptions: {}
    });
    mongog.startSet();
    mongog.initiate();
    mongog.getPrimary().getDB("test").setLogLevel(2); 
    
    jsTestLog(" started MongoG replicaset");

    const mongo_dest = new ReplSetTest({
        name: "dest_rs",
        nodes: [
            {setParameter: {
               isMongodWithGlobalSync: true,
               testSourceRSPort: mongog.getPrimary().port}}],
        nodeOptions: {}
    });
    mongo_dest.startSet();
    mongo_dest.initiate();
    mongo_dest.getPrimary().getDB("test").setLogLevel(2); 
    let destColl = mongo_dest.getPrimary().getDB(dbName)[collName];
    assert.commandWorked(destColl.insert({_id: 0}, {writeConcern: {w: "majority"}}));
    
    jsTestLog(" started dest replicaset");
    // Insert a document on primary and let it majority commit.
    sleep(1000);
    let globalDB = mongog.getPrimary().getDB("local");
    let res = assert.commandWorked(globalDB.runCommand({count: "oplog_global"}));
    assert.eq(res.n, 0);

    const nDocs = 10;
    for (let i=1; i<=nDocs; ++i) {
        assert.commandWorked(primaryColl.insert({_id: i}, {writeConcern: {w: "majority"}}));
    }
    sleep(1000);
    res = assert.commandWorked(globalDB.runCommand({count: "oplog_global"}));
    assert.eq(res.n, nDocs);

    jsTestLog("waiting for dest replicaset to catch up");
    sleep(1000);

    let destDB = mongo_dest.getPrimary().getDB(dbName);
    res = assert.commandWorked(destDB.runCommand({count: collName}));
    jsTestLog("just checking: res: " + tojson(res));
    assert.eq(res.n, nDocs);

    replTestSource.stopSet();
    mongog.stopSet();
    mongo_dest.stopSet();
})();
