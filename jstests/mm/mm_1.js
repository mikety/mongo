/**
 * Test multi-master repl to mongog.
 */
(function() {
    "use strict";

    load("jstests/libs/write_concern_util.js");  // for [stop|restart]ServerReplication.
    const dbName = "mm_replication";
    const collName = "MultiMasterCollection";

    const source_rs = new ReplSetTest({
        name: "source_rs",
        nodes: [{setParameter: {isMongodWithGlobalSync: true}}],
        nodeOptions: {}
    });
    source_rs.startSet();
    source_rs.initiate();
    jsTestLog(" started source replicaset");

    let sourceDB = source_rs.getPrimary().getDB(dbName);
    let sourceLocalDB = source_rs.getPrimary().getDB("local");
    let sourceAdminDB = source_rs.getPrimary().getDB("admin");
    let sourceColl = sourceDB[collName];

    assert.commandWorked(sourceDB.createCollection(collName));

    jsTestLog("primed source collection");

    const mongog = new ReplSetTest({
        name: "mongog_rs",
        nodes: [{setParameter: {isMongoG: true, testSourceRSPort: source_rs.getPrimary().port}}],
        nodeOptions: {}
    });
    mongog.startSet();
    mongog.initiate();
    // mongog.getPrimary().getDB("test").setLogLevel(2);
    jsTestLog(" started MongoG replicaset");

    let globalDB = mongog.getPrimary().getDB("local");
    globalDB.createCollection("oplog_global", {capped: true, size: 100000});

    assert.commandWorked(
        sourceAdminDB.runCommand({setParameter: 1, testSourceRSPort: mongog.getPrimary().port}));

    const dest_rs = new ReplSetTest({
        name: "dest_rs",
        nodes: [{
            setParameter:
                {isMongodWithGlobalSync: true, testSourceRSPort: mongog.getPrimary().port}
        }],
        nodeOptions: {}
    });

    dest_rs.startSet();
    dest_rs.initiate();
    // dest_rs.getPrimary().getDB("test").setLogLevel(2);
    jsTestLog(" started dest replicaset");

    let destDB = dest_rs.getPrimary().getDB(dbName);
    let destLocalDB = dest_rs.getPrimary().getDB("local");
    let destColl = destDB[collName];

    assert.commandWorked(destDB.createCollection(collName));
    jsTestLog("primed dest collection");

    // Insert a document on primary and let it majority commit.
    sleep(1000);

    let res = assert.commandWorked(globalDB.runCommand({count: "oplog_global"}));
    assert.eq(res.n, 0);

    const nDocs = 10;
    for (let i = 1; i <= nDocs; ++i) {
        assert.commandWorked(sourceColl.insert({_id: i}, {writeConcern: {w: "majority"}}));
    }
    sleep(1000);
    // res = assert.commandWorked(globalDB.runCommand({count: "oplog_global"}));
    // assert.eq(res.n, nDocs);

    jsTestLog("waiting for dest replicaset to catch up");
    sleep(1000);

    res = assert.commandWorked(destDB.runCommand({count: collName}));
    jsTestLog("just checking: res: " + tojson(res));
    // assert.eq(res.n, nDocs);

    // Now insert into the dest and check out the source
    assert.commandWorked(mongog.getPrimary().getDB("admin").runCommand(
        {setParameter: 1, testSourceRSPort: dest_rs.getPrimary().port}));

    for (let i = 11; i <= nDocs + 10; ++i) {
        assert.commandWorked(destColl.insert({_id: i}, {writeConcern: {w: "majority"}}));
    }

    jsTestLog("waiting for source replicaset to catch up");
    sleep(10000);

    res = assert.commandWorked(globalDB.runCommand({find: "oplog_global"}));
    jsTestLog("just checking: res global: " + tojson(res));

    res = assert.commandWorked(destDB.runCommand({find: collName}));
    jsTestLog("just checking: res dest: " + tojson(res));
    res = assert.commandWorked(destLocalDB.runCommand({find: "oplog.rs"}));
    jsTestLog("just checking: dest oplog: " + tojson(res));

    res = assert.commandWorked(sourceDB.runCommand({find: collName}));
    jsTestLog("just checking: res source: " + tojson(res));
    res = assert.commandWorked(sourceLocalDB.runCommand({find: "oplog.rs"}));
    jsTestLog("just checking: source oplog: " + tojson(res));

    source_rs.stopSet();
    mongog.stopSet();
    dest_rs.stopSet();
})();
