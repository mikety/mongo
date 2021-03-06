/**
 * Test multi-master repl to mongog.
 */
(function() {
    "use strict";

    load("jstests/libs/write_concern_util.js");  // for [stop|restart]ServerReplication.
    const dbName = "mm_replication";
    const collName = "MultiMasterCollection";
    const conflictsCollName = "MultiMasterCollection.conflicts";

    let st = new ShardingTest({
        name: "Xshard",
        shards: 3,
        rs: {nodes: 1},
        other: {rsOptions: {setParameter: {isMongodWithGlobalSync: true}}}
    });

    let globalPrimary = st.rs0.getPrimary();
    let globalPort = globalPrimary.port;

    assert.commandWorked(
        globalPrimary.getDB("admin").runCommand({setParameter: 1, isMongoG: true}));

    assert.commandWorked(
        st.rs0.getPrimary().getDB("admin").runCommand({setParameter: 1, mmPortConfig: globalPort}));
    assert.commandWorked(
        st.rs1.getPrimary().getDB("admin").runCommand({setParameter: 1, mmPortConfig: globalPort}));
    assert.commandWorked(
        st.rs2.getPrimary().getDB("admin").runCommand({setParameter: 1, mmPortConfig: globalPort}));

    assert.commandWorked(globalPrimary.getDB("admin").runCommand(
        {setParameter: 1, mmPort1: st.rs0.getPrimary().port}));
    assert.commandWorked(globalPrimary.getDB("admin").runCommand(
        {setParameter: 1, mmPort2: st.rs1.getPrimary().port}));
    assert.commandWorked(globalPrimary.getDB("admin").runCommand(
        {setParameter: 1, mmPort3: st.rs2.getPrimary().port}));

    jsTestLog("Setting up Global Oplog");
    let globalDB = globalPrimary.getDB("local");
    globalDB.createCollection("oplog_global", {capped: true, size: 100000});
    let res = assert.commandWorked(globalDB.runCommand({count: "oplog_global"}));
    assert.eq(res.n, 0);

    jsTestLog("Setting up Global Shards");
    assert.commandWorked(st.shard0.getDB(dbName).createCollection(collName));
    assert.commandWorked(st.shard0.getDB(dbName).createCollection(conflictsCollName));
    assert.commandWorked(st.shard1.getDB(dbName).createCollection(collName));
    assert.commandWorked(st.shard1.getDB(dbName).createCollection(conflictsCollName));
    assert.commandWorked(st.shard2.getDB(dbName).createCollection(collName));
    assert.commandWorked(st.shard2.getDB(dbName).createCollection(conflictsCollName));

    assert.commandWorked(st.shard0.getDB("admin").runCommand({replSetStartGlobalSync: 1}));

    assert.commandWorked(st.shard1.getDB("admin").runCommand({replSetStartGlobalSync: 1}));

    assert.commandWorked(st.shard2.getDB("admin").runCommand({replSetStartGlobalSync: 1}));

    let shard0coll = st.shard0.getDB(dbName).getCollection(collName);
    let shard1coll = st.shard1.getDB(dbName).getCollection(collName);
    jsTestLog("Inserting in the shard0 docs X");
    const nDocs = 3;
    for (let i = 1; i <= nDocs; ++i) {
        assert.commandWorked(shard0coll.insert({_id: i, X: "apple", Y: "orange"}));
    }
    sleep(3000);

    jsTestLog("Updating on the shard1 docs X");
    for (let i = 1; i <= nDocs; ++i) {
        assert.commandWorked(shard1coll.updateOne({_id: i}, {$set: {Y: "onion"}}));
    }
    // sleep(1000);

    jsTestLog("Conflict: Updating on the shard0 docs X");
    for (let i = 1; i <= nDocs; ++i) {
        assert.commandWorked(shard0coll.updateOne({_id: i}, {$set: {X: "garlic"}}));
    }
    sleep(3000);

    res = assert.commandWorked(globalDB.runCommand({find: "oplog_global"}));
    jsTestLog("Printing: Global Oplog: " + tojson(res));

    res = assert.commandWorked(st.rs0.getPrimary().getDB("local").runCommand({find: "oplog.rs"}));
    jsTestLog("Printing: Shard0  Oplog: " + tojson(res));

    res = assert.commandWorked(st.shard0.getDB(dbName).runCommand({find: collName}));
    jsTestLog("Printing: Shard0  Data: " + tojson(res));

    res = assert.commandWorked(st.shard0.getDB(dbName).runCommand({find: conflictsCollName}));
    jsTestLog("Printing: Shard0  Conflicts: " + tojson(res));

    res = assert.commandWorked(st.rs1.getPrimary().getDB("local").runCommand({find: "oplog.rs"}));
    jsTestLog("Printing: Shard1  Oplog: " + tojson(res));

    res = assert.commandWorked(st.shard1.getDB(dbName).runCommand({find: collName}));
    jsTestLog("Printing: Shard1  Data: " + tojson(res));

    res = assert.commandWorked(st.shard1.getDB(dbName).runCommand({find: conflictsCollName}));
    jsTestLog("Printing: Shard1  Conflicts: " + tojson(res));

    res = assert.commandWorked(st.rs2.getPrimary().getDB("local").runCommand({find: "oplog.rs"}));
    jsTestLog("Printing: Shard2  Oplog: " + tojson(res));

    res = assert.commandWorked(st.shard2.getDB(dbName).runCommand({find: collName}));
    jsTestLog("Printing: Shard2  Data: " + tojson(res));

    res = assert.commandWorked(st.shard2.getDB(dbName).runCommand({find: conflictsCollName}));
    jsTestLog("Printing: Shard2  Conflicts: " + tojson(res));

    sleep(1000000000);
    // st.stop();
})();
