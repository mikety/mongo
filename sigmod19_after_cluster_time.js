/**
 * Tests readConcern: afterClusterTime behavior in a sharded cluster.
 * @tags: [requires_majority_read_concern]
 */
(function() {
    "use strict";

    load("jstests/replsets/rslib.js");  // For startSetIfSupportsReadMajority.

    const testSetSize = 1000;
    // Reads with proper afterClusterTime arguments return committed data after the given time.
    let testReadOwnWrite = function(db, val, readConcern) {
        return assert.commandWorked(db.runReadCommand({find: "foo", filter: {x: "X"}, readConcern: {level: readConcern}}), 
            "error in read " + val);
    };

    const rst = new ReplSetTest({ nodes: 5 });

    rst.startSet();
    rst.initiate();

    const primary = rst.getPrimary();
    /*
    const secondary = rst.getSecondary();
    primary.setSlaveOk(false);
    primary.setReadPref("primary");
   */

    const session1 = primary.startSession({causalConsistency: false});
    // const session2 = secondary.startSession({causalConsistency: true});

    let testDB = session1.getDatabase("test");

    let startTime = Date.now();
    for (let i = 1; i < testSetSize; i++) {
        let res = testDB.runCommand( {insert: "foo", documents: [{_id: i, x: "X"}], writeConcern: {w: 1}}); 
        assert.commandWorked(res, "expected insert to to succeed");

        /*
        session2.advanceClusterTime(session1.getClusterTime());
        session2.advanceOperationTime(session1.getOperationTime());
       */

        testReadOwnWrite(session1.getDatabase(testDB.getName()), i, "majority");
    }
    let totalTime = Date.now() - startTime;

    rst.stopSet();

    print("=============== read you write for " + testSetSize + " documents took " + totalTime + " milliseconds ============");
})();

