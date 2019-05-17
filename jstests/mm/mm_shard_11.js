/**
 * Test multi-master repl to mongog.
 */
(function() {
    "use strict";

    load("jstests/libs/write_concern_util.js");  // for [stop|restart]ServerReplication.
    const dbName = "mm_replication";
    // const collName = "MultiMasterCollection";
    const collName = "MMColl1";
    const collName2 = "MMColl2";
    const conflictsCollName = collName + ".conflicts";
    const conflictsViewName = collName + ".view";
    const pipeline = []; 

    let st = new ShardingTest({
        name: "Xshard",
        shards: 3,
        rs: {nodes: 1},
        other: {rsOptions: {setParameter: {isMongodWithGlobalSync: true}}}
    });

    let globalPrimary = st.rs0.getPrimary();
    let globalPort = globalPrimary.port;
    const mongos = st.s0;

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

    jsTestLog("create global collection");
    assert.commandWorked(
        mongos.getDB(dbName).runCommand({create: collName, global: true, sharded: false}));
//     assert.commandWorked(st.shard0.getDB(dbName).createCollection(conflictsCollName));
    //assert.commandWorked(st.shard0.getDB(dbName).createView(conflictsViewName, conflictsCollName, pipeline));
    sleep(1000);
    // assert.commandWorked(mongos.getDB(dbName).runCommand({create: collName2, global: true,
    // sharded: false}));
    // sleep(1000);

    assert.commandWorked(st.shard0.getDB("admin").runCommand({replSetStartGlobalSync: 1}));
    assert.commandWorked(st.shard1.getDB("admin").runCommand({replSetStartGlobalSync: 1}));
    assert.commandWorked(st.shard2.getDB("admin").runCommand({replSetStartGlobalSync: 1}));

    sleep(3000);

    let shard0coll = st.shard0.getDB(dbName).getCollection(collName);
    let shard0coll2 = st.shard0.getDB(dbName).getCollection(collName2);
    let shard1coll = st.shard1.getDB(dbName).getCollection(collName);
    let shard2coll = st.shard2.getDB(dbName).getCollection(collName);
    jsTestLog("Inserting in the shard0 docs X");
    const nDocs = 3;
    for (let i = 1; i <= nDocs; ++i) {
        assert.commandWorked(shard0coll.insert({_id: i, X: "apple", Y: "orange"}));
    }
    /*
    for (let i = 1; i <= nDocs; ++i) {
        assert.commandWorked(shard0coll2.insert({_id: i, X: "avocado", Y: "chocolate"}));
    }
   */
    sleep(5000);

    jsTestLog("Updating on the shard1 docs X");
    for (let i = 1; i <= nDocs; ++i) {
        assert.commandWorked(shard1coll.updateOne({_id: i}, {$set: {Y: "onion"}}));
    }

    jsTestLog("Conflict: Updating on the shard0 docs X");
    for (let i = 1; i <= nDocs; ++i) {
        assert.commandWorked(shard0coll.updateOne({_id: i}, {$set: {X: "garlic"}}));
    }
    sleep(5000);

    res = assert.commandWorked(globalDB.runCommand({find: "oplog_global"}));
    jsTestLog("Printing: Global Oplog: " + tojson(res));

    res = assert.commandWorked(st.rs0.getPrimary().getDB("local").runCommand({find: "oplog.rs"}));
    jsTestLog("Printing: Shard0  Oplog: " + tojson(res));

    res = assert.commandWorked(st.shard0.getDB(dbName).runCommand({find: collName}));
    jsTestLog("Printing: Shard0  Coll1 Data: " + tojson(res));

    /*
    res = assert.commandWorked(st.shard0.getDB(dbName).runCommand({find: collName2}));
    jsTestLog("Printing: Shard0  Coll2 Data: " + tojson(res));
   */

    res = assert.commandWorked(st.shard0.getDB(dbName).runCommand({find: conflictsCollName}));
    jsTestLog("Printing: Shard0  Conflicts: " + tojson(res));

    res = assert.commandWorked(st.rs1.getPrimary().getDB("local").runCommand({find: "oplog.rs"}));
    jsTestLog("Printing: Shard1  Oplog: " + tojson(res));

    res = assert.commandWorked(st.shard1.getDB(dbName).runCommand({find: collName}));
    jsTestLog("Printing: Shard1  Coll1 Data: " + tojson(res));

    /*
    res = assert.commandWorked(st.shard1.getDB(dbName).runCommand({find: collName2}));
    jsTestLog("Printing: Shard1  Coll2 Data: " + tojson(res));
   */

    res = assert.commandWorked(st.shard1.getDB(dbName).runCommand({find: conflictsCollName}));
    jsTestLog("Printing: Shard1  Conflicts: " + tojson(res));

    res = assert.commandWorked(st.rs2.getPrimary().getDB("local").runCommand({find: "oplog.rs"}));
    jsTestLog("Printing: Shard2  Oplog: " + tojson(res));

    res = assert.commandWorked(st.shard2.getDB(dbName).runCommand({find: collName}));
    jsTestLog("Printing: Shard2  Coll1 Data: " + tojson(res));

    /*
    res = assert.commandWorked(st.shard2.getDB(dbName).runCommand({find: collName2}));
    jsTestLog("Printing: Shard2  Coll2 Data: " + tojson(res));
    */

    res = assert.commandWorked(st.shard2.getDB(dbName).runCommand({find: conflictsCollName}));
    jsTestLog("Printing: Shard2  Conflicts: " + tojson(res));

    sleep(1000000000);
    // st.stop();
})();
