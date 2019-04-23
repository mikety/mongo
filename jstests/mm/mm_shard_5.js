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

    let mongoGPort1 = st.rs1.getPrimary().port;
    let mongoGPort2 = st.rs2.getPrimary().port;

    assert.commandWorked(st.rs0.getPrimary().getDB("admin").runCommand(
        {setParameter: 1, mmPortConfig: mongoGPort1}));
    assert.commandWorked(st.rs1.getPrimary().getDB("admin").runCommand(
        {setParameter: 1, mmPortConfig: mongoGPort1}));
    assert.commandWorked(st.rs2.getPrimary().getDB("admin").runCommand(
        {setParameter: 1, mmPortConfig: mongoGPort2}));

    assert.commandWorked(st.rs2.getPrimary().getDB("admin").runCommand(
        {setParameter: 1, mmPort1: st.rs0.getPrimary().port}));
    assert.commandWorked(st.rs2.getPrimary().getDB("admin").runCommand(
        {setParameter: 1, mmPort2: st.rs1.getPrimary().port}));
    assert.commandWorked(st.rs2.getPrimary().getDB("admin").runCommand(
        {setParameter: 1, mmPort3: st.rs2.getPrimary().port}));
    assert.commandWorked(
        st.rs2.getPrimary().getDB("admin").runCommand({setParameter: 1, isMongoG: true}));

    assert.commandWorked(st.rs1.getPrimary().getDB("admin").runCommand(
        {setParameter: 1, mmPort1: st.rs0.getPrimary().port}));
    assert.commandWorked(st.rs1.getPrimary().getDB("admin").runCommand(
        {setParameter: 1, mmPort2: st.rs1.getPrimary().port}));
    assert.commandWorked(st.rs1.getPrimary().getDB("admin").runCommand(
        {setParameter: 1, mmPort3: st.rs2.getPrimary().port}));
    assert.commandWorked(
        st.rs1.getPrimary().getDB("admin").runCommand({setParameter: 1, isMongoG: true}));

    jsTestLog("Setting up Global Oplog");
    let globalDB2 = st.rs2.getPrimary().getDB("local");
    globalDB2.createCollection("oplog_global", {capped: true, size: 100000});
    let res = assert.commandWorked(globalDB2.runCommand({count: "oplog_global"}));
    assert.eq(res.n, 0);

    let globalDB1 = st.rs1.getPrimary().getDB("local");
    globalDB1.createCollection("oplog_global", {capped: true, size: 100000});
    res = assert.commandWorked(globalDB1.runCommand({count: "oplog_global"}));
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

    jsTestLog("Inserting in the shard1 docs Y");
    for (let i = nDocs + 1; i <= nDocs + nDocs; ++i) {
        assert.commandWorked(shard1coll.insert({_id: i, X: "cherry", Y: "banana"}));
    }
    sleep(3000);

    /*
    jsTestLog("Updating on the shard1 docs X");
    for (let i = 1; i <= nDocs; ++i) {
        assert.commandWorked(shard1coll.updateOne({_id: i}, {$set: {X: "X_1_1"}}));
    }
    sleep(5000);
   */

    jsTestLog("Updating on the shard1 docs X");
    for (let i = 1; i <= nDocs; ++i) {
        assert.commandWorked(shard1coll.updateOne({_id: i}, {$set: {X: "onion"}}));
    }
    sleep(3000);

    jsTestLog("Conflict: Updating on the shard0 docs Y");
    for (let i = nDocs + 1; i <= nDocs + nDocs; ++i) {
        assert.commandWorked(shard0coll.updateOne({_id: i}, {$set: {Y: "garlic"}}));
    }
    sleep(3000);

    assert.commandWorked(shard1coll.remove({_id: 1}));
    sleep(3000);

    assert.commandWorked(shard0coll.remove({_id: nDocs + 1}));
    sleep(3000);
    /*
    jsTestLog("Conflict: Updating on the shard0 docs X");
    for (let i = 1; i <= nDocs; ++i) {
        assert.commandWorked(shard0coll.updateOne({_id: i}, {$set: {X: "X_0_2"}}));
    }
    sleep(5000);
   */

    res = assert.commandWorked(globalDB1.runCommand({find: "oplog_global"}));
    jsTestLog("Printing: Global Oplog 1: " + tojson(res));

    res = assert.commandWorked(globalDB2.runCommand({find: "oplog_global"}));
    jsTestLog("Printing: Global Oplog 2: " + tojson(res));

    res = assert.commandWorked(st.rs0.getPrimary().getDB("local").runCommand({find: "oplog.rs"}));
    jsTestLog("Printing: Shard0  Oplog: " + tojson(res));

    res = assert.commandWorked(st.shard0.getDB(dbName).runCommand({find: collName}));
    jsTestLog("Printing: Shard0  Data: " + tojson(res));

    res = assert.commandWorked(st.rs1.getPrimary().getDB("local").runCommand({find: "oplog.rs"}));
    jsTestLog("Printing: Shard1  Oplog: " + tojson(res));

    res = assert.commandWorked(st.shard1.getDB(dbName).runCommand({find: collName}));
    jsTestLog("Printing: Shard1  Data: " + tojson(res));

    res = assert.commandWorked(st.rs2.getPrimary().getDB("local").runCommand({find: "oplog.rs"}));
    jsTestLog("Printing: Shard2  Oplog: " + tojson(res));

    res = assert.commandWorked(st.shard2.getDB(dbName).runCommand({find: collName}));
    jsTestLog("Printing: Shard2  Data: " + tojson(res));

    // sleep(1000000000);
    st.stop();
})();
