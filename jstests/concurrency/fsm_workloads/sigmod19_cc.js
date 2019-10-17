'use strict';

/**
 * secondary_reads.js
 *
 * One thread (tid 0) is dedicated to writing documents with field 'x' in
 * ascending order into the collection.
 *
 * Other threads do one of the following operations each iteration.
 * 1) Retrieve first 50 documents in descending order with local readConcern from a secondary node.
 * 2) Retrieve first 50 documents in descending order with available readConcern from a secondary
 * node.
 * 3) Retrieve first 50 documents in descending order with majority readConcern from a secondary
 * node.
 *
 * For each read, we check if there is any 'hole' in the returned batch. There
 * should not be any 'hole' because oplogs are applied sequentially in batches.
 *
 * @tags: [requires_replication]
 */

var $config = (function() {

    // Use the workload name as the collection name.
    var uniqueCollectionName = 'sigmod19_test';
    var nThreads = 10;
    var replicaSetSize = 3;
    var isCausalConsistency = true;

    load('jstests/concurrency/fsm_workload_helpers/server_types.js');

    function isWriterThread() {
        // return this.tid === 0;
        return false;
    }

    function insertDocuments(db, collName, writeConcern) {
        let bulk = db[collName].initializeOrderedBulkOp();
        let threadRange = this.tid * 100000;
        let insertedVal = undefined;
        for (let i = threadRange + this.nDocumentsInTotal;
             i < threadRange + this.nDocumentsInTotal + this.nDocumentsToInsert;
             i++) {
            bulk.insert({_id: i, x: i});
            insertedVal = i;
        }
        let res = bulk.execute(writeConcern);
        assertWhenOwnColl.writeOK(res);
        assertWhenOwnColl.eq(this.nDocumentsToInsert, res.nInserted);
        this.nDocumentsInTotal += this.nDocumentsToInsert;
        return insertedVal;
    }

    function readFromSecondaries(db, collName, val) {
        let arr = [];
        let success = false;
        while (!success) {
            try {
                // expencive read - also possible to make it chep by looking up by the val and not
                // making the sort
                arr = db[collName]
                          .find({})
                          //.find({_id: val})
                          .readPref(this.readPreference)
                          .readConcern(this.readConcernLevel)
                          .limit(this.nDocumentsToCheck)
                          .sort({_id: 1, x: 1})
                          .toArray();
                success = true;
            } catch (e) {
                // We propagate TransientTransactionErrors to allow the state function to
                // automatically be retried when TestData.runInsideTransaction=true
                if (e.hasOwnProperty('errorLabels') &&
                    e.errorLabels.includes('TransientTransactionError')) {
                    throw e;
                }
                // Retry if the query is interrupted.
                assertAlways.eq(e.code,
                                ErrorCodes.QueryPlanKilled,
                                'unexpected error code: ' + e.code + ': ' + e.message);
            }
        }
    }

    var states = (function() {

        // One thread is dedicated to writing and other threads perform reads on
        // secondaries with a randomly chosen readConcern level.
        function readFromSecondaries(db, collName) {
            if (this.isWriterThread()) {
                this.insertDocuments(db, this.collName);
            } else {
                let session =
                    db.getMongo().startSession({causalConsistency: this.isCausalConsistency});
                db = session.getDatabase(db.getName());
                let startTime = Date.now();
                for (let i = 0; i < this.nCycles; i++) {
                    let insertedVal =
                        this.insertDocuments(db, this.collName, {w: this.writeConcern});
                    this.readFromSecondaries(db, this.collName, insertedVal);
                }
                let total = Date.now() - startTime;
                let resultsConn = new Mongo("mongodb://127.0.0.1:27017/test");
                let resultsDB = resultsConn.getDB("sigmod19");
                resultsDB["resultsColl"].insert({
                    threadId: this.tid,
                    nThreads: this.nThreads,
                    isCausalConsistency: this.isCausalConsistency,
                    writeConcern: this.writeConcern,
                    readConcernLevel: this.readConcernLevel,
                    readPreference: this.readPreference,
                    nInsertBatch: this.nDocumentsToInsert,
                    replicaSetSize: replicaSetSize,
                    findOperator: "eq",
                    hasIndex: true,
                    hasSort: true,
                    totalTime: total
                });
            }
        }

        return {readFromSecondaries: readFromSecondaries};
    })();

    var transitions = {readFromSecondaries: {readFromSecondaries: 1}};

    var setup = function setup(db, collName, cluster) {
        this.nDocumentsInTotal = 0;
        this.nCycles = 1000;
        // Start write workloads to activate oplog application on secondaries
        // before any reads.
        // this.insertDocuments(db, this.collName, {w: this.writeConcern});
        // Facilitate sorting by "x".
        assert.commandWorked(db[collName].createIndex({x: 1}));
    };

    return {
        threadCount: nThreads,
        iterations: 1,
        startState: 'readFromSecondaries',
        states: states,
        data: {
            nThreads: nThreads,
            isCausalConsistency: isCausalConsistency,
            readPreference: isCausalConsistency ? "secondary" : "primary",
            writeConcern: "majority",
            readConcernLevel: "majority",
            nDocumentsToInsert: 100,
            nDocumentsToCheck: 1,
            isWriterThread: isWriterThread,
            insertDocuments: insertDocuments,
            readFromSecondaries: readFromSecondaries,
            collName: uniqueCollectionName
        },
        transitions: transitions,
        setup: setup,
    };
})();
