/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding copyright
 * ownership.  The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of
 * the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

/*
 * In-memory columnar table access method.
 *
 * The initial implementation started with blackhole_am by Michael
 * Paquier, but heavily modified to create an in-memory implementation
 * of a table.
 *
 * This is mostly intended to be used for experimentation with and
 * learning about the internals of PostgreSQL, with special focus on
 * the access methods. As such it is heavily sprinkled with debug
 * printouts.
 *
 * The memory format is based on the Arrow C data structure
 * (ArrowArray and ArrowSchema structures), with some additions.
 */

#include "arrowam_handler.h"

#include <postgres.h>

#include <access/amapi.h>
#include <access/heapam.h>
#include <access/tableam.h>
#include <access/xact.h>
#include <catalog/index.h>
#include <commands/tablespace.h>
#include <commands/vacuum.h>
#include <executor/tuptable.h>
#include <miscadmin.h>
#include <storage/predicate.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/snapmgr.h>

#include <math.h>

#include "arrow_array.h"
#include "arrow_scan.h"
#include "arrow_storage.h"
#include "arrow_tts.h"
#include "debug.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(arrowam_handler);

void _PG_init(void);

static const TableAmRoutine arrowam_methods;

static const TupleTableSlotOps *arrowam_slot_callbacks(Relation relation) {
  return &TTSOpsArrowTuple;
}

static TableScanDesc arrowam_scan_begin(Relation relation, Snapshot snapshot,
                                        int nkeys, ScanKey key,
                                        ParallelTableScanDesc parallel_scan,
                                        uint32 flags) {
  ArrowScanDesc *scan = NULL;
  Oid relid;
  const char *relname;

  RelationIncrementReferenceCount(relation);

  relid = RelationGetRelid(relation);
  relname = RelationGetRelationName(relation);

  DEBUG_ENTER("relation: %s.%s, relid: %u, nkeys: %d, snapshot: %s",
              get_namespace_name(RelationGetNamespace(relation)), relname,
              relid, nkeys, ExportSnapshot(snapshot));

  scan = (ArrowScanDesc *)palloc0(sizeof(ArrowScanDesc));

  scan->base.rs_rd = relation;
  scan->base.rs_snapshot = snapshot;
  scan->base.rs_nkeys = nkeys;
  scan->base.rs_flags = flags;
  scan->base.rs_parallel = parallel_scan;

  scan->index = 0;
  scan->length = -1;

  if (flags & (SO_TYPE_SEQSCAN | SO_TYPE_SAMPLESCAN)) {
    /*
     * Ensure a missing snapshot is noticed reliably, even if the
     * isolation mode means predicate locking isn't performed (and
     * therefore the snapshot isn't used here).
     */
    Assert(snapshot);
    PredicateLockRelation(relation, snapshot);
  }

  DEBUG_LEAVE("relation: %s, relid: %u", relname, relid);

  return (TableScanDesc)scan;
}

static void arrowam_scan_end(TableScanDesc sscan) {
  ArrowScanDesc *scan = (ArrowScanDesc *)sscan;
  DEBUG_ENTER("");

  RelationDecrementReferenceCount(scan->base.rs_rd);
  pfree(scan);

  DEBUG_LEAVE("");
}

static void arrowam_scan_rescan(TableScanDesc scan, ScanKey key,
                                bool set_params, bool allow_strat,
                                bool allow_sync, bool allow_pagemode) {
  ArrowScanDesc *ascan = (ArrowScanDesc *)scan;
  ascan->index = 0;
}

static bool arrowam_scan_getnextslot(TableScanDesc scan,
                                     ScanDirection direction,
                                     TupleTableSlot *slot) {
  ArrowScanDesc *ascan = (ArrowScanDesc *)scan;
  ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *)slot;

  DEBUG_ENTER("scan.index: %ld, scan.length: %ld, tts_tableOid: %d",
              ascan->index, ascan->length, slot->tts_tableOid);

  if (ascan->length >= 0 && ascan->index == ascan->length)
    return false;

  /*
   * Open first segment if scan descriptor not initialized.
   *
   * We assume that there is at least one column in the table. We need
   * this to get the length of the array.
   *
   * TODO: We could also use a segment zero to store xmin and xmax as
   * a structure, which might be needed to support MVCC and repeatable
   * read isolation, but right now we do not have support for storing
   * structures in arrays.
   */
  if (ascan->length == -1) {
    aslot->columns[0] = ArrowArrayGet(
        scan->rs_rd->rd_id, &slot->tts_tupleDescriptor->attrs[0], O_RDWR);
    ascan->length = aslot->columns[0]->length;
    aslot->base.tts_nvalid = 1;
  }

  aslot->index = ascan->index++;
  slot->tts_nvalid = 0;
  slot->tts_flags &= ~TTS_FLAG_EMPTY;

  DEBUG_LOG("slot.index: %ld, slot.tts_nvalid: %d", aslot->index,
            aslot->base.tts_nvalid);

  DEBUG_LEAVE("scan.index: %ld, scan.length: %ld, more: %s", ascan->index,
              ascan->length, YESNO(ascan->index < ascan->length));

  return true;
}

static IndexFetchTableData *arrowam_index_fetch_begin(Relation relation) {
  return NULL;
}

static void arrowam_index_fetch_reset(IndexFetchTableData *scan) {
  /* nothing to do here */
}

static void arrowam_index_fetch_end(IndexFetchTableData *scan) {
  /* nothing to do here */
}

static __attribute__((unused)) const char *show_tid(ItemPointer tid) {
  static char buf[32];
  snprintf(buf, sizeof(buf), "{ip_blkid=%d:%d,ip_posid=%d}",
           tid->ip_blkid.bi_hi, tid->ip_blkid.bi_lo, tid->ip_posid);
  return buf;
}

static bool arrowam_index_fetch_tuple(struct IndexFetchTableData *scan,
                                      ItemPointer tid, Snapshot snapshot,
                                      TupleTableSlot *slot, bool *call_again,
                                      bool *all_dead) {
  return 0;
}

/* ------------------------------------------------------------------------
 * Callbacks for non-modifying operations on individual tuples for
 * memory AM.
 * ------------------------------------------------------------------------
 */

static bool arrowam_fetch_row_version(Relation relation, ItemPointer tid,
                                      Snapshot snapshot, TupleTableSlot *slot) {
  return false;
}

static void arrowam_get_latest_tid(TableScanDesc sscan, ItemPointer tid) {
  /* nothing to do */
}

static bool arrowam_tuple_tid_valid(TableScanDesc scan, ItemPointer tid) {
  return false;
}

static bool arrowam_tuple_satisfies_snapshot(Relation relation,
                                             TupleTableSlot *slot,
                                             Snapshot snapshot) {
  return false;
}

static TransactionId arrowam_index_delete_tuples(Relation rel,
                                                 TM_IndexDeleteOp *delstate) {
  TransactionId snapshotConflictHorizon = InvalidTransactionId;
  DEBUG_ENTER("relation: %s.%s", get_namespace_name(RelationGetNamespace(rel)),
              RelationGetRelationName(rel));

  DEBUG_LEAVE("relation: %s.%s", get_namespace_name(RelationGetNamespace(rel)),
              RelationGetRelationName(rel));

  return snapshotConflictHorizon;
}

static void arrowam_tuple_insert(Relation relation, TupleTableSlot *slot,
                                 CommandId cid, int options,
                                 BulkInsertState bistate) {
  const Oid relid = RelationGetRelid(relation);
  DEBUG_ENTER("relation: %s.%s, slot: %s",
              get_namespace_name(RelationGetNamespace(relation)),
              RelationGetRelationName(relation), show_slot(slot)->data);

  ExecInsertArrowSlot(relation, relid, slot, cid, options);

  DEBUG_LEAVE("relation: %s.%s",
              get_namespace_name(RelationGetNamespace(relation)),
              RelationGetRelationName(relation));
}
static void arrowam_tuple_insert_speculative(Relation relation,
                                             TupleTableSlot *slot,
                                             CommandId cid, int options,
                                             BulkInsertState bistate,
                                             uint32 specToken) {
  /* nothing to do */
}

static void arrowam_tuple_complete_speculative(Relation relation,
                                               TupleTableSlot *slot,
                                               uint32 spekToken,
                                               bool succeeded) {
  /* nothing to do */
}

static void arrowam_multi_insert(Relation relation, TupleTableSlot **slots,
                                 int ntuples, CommandId cid, int options,
                                 BulkInsertState bistate) {
  /* nothing to do */
}

static TM_Result arrowam_tuple_delete(Relation relation, ItemPointer tid,
                                      CommandId cid, Snapshot snapshot,
                                      Snapshot crosscheck, bool wait,
                                      TM_FailureData *tmfd, bool changingPart) {
  DEBUG_ENTER("relation: %s.%s, snapshot: %s, ",
              get_namespace_name(RelationGetNamespace(relation)),
              RelationGetRelationName(relation), ExportSnapshot(snapshot));
  DEBUG_LEAVE("relation: %s.%s, snapshot: %s, ",
              get_namespace_name(RelationGetNamespace(relation)),
              RelationGetRelationName(relation), ExportSnapshot(snapshot));
  return TM_Ok;
}

static TM_Result arrowam_tuple_update(Relation rel, ItemPointer otid,
                                      TupleTableSlot *slot, CommandId cid,
                                      Snapshot snapshot, Snapshot crosscheck,
                                      bool wait, TM_FailureData *tmfd,
                                      LockTupleMode *lockmode,
                                      TU_UpdateIndexes *update_indexes) {
  const char *nsname = get_namespace_name(RelationGetNamespace(rel));
  const char *relname = RelationGetRelationName(rel);
  DEBUG_ENTER("relation: %s.%s, snapshot: %s, ", nsname, relname,
              ExportSnapshot(snapshot));
  DEBUG_LEAVE("relation: %s.%s, snapshot: %s, ", nsname, relname,
              ExportSnapshot(snapshot));
  return TM_Ok;
}

static TM_Result arrowam_tuple_lock(Relation relation, ItemPointer tid,
                                    Snapshot snapshot, TupleTableSlot *slot,
                                    CommandId cid, LockTupleMode mode,
                                    LockWaitPolicy wait_policy, uint8 flags,
                                    TM_FailureData *tmfd) {
  return TM_Ok;
}

static void arrowam_finish_bulk_insert(Relation relation, int options) {
  /* nothing to do */
}

static void arrowam_relation_set_new_filelocator(
    Relation relation, const RelFileLocator *newrlocator, char persistence,
    TransactionId *freezeXid, MultiXactId *minmulti) {
  TupleDesc tupdesc;
  DEBUG_ENTER("relation: %s.%s, node.tablespace: %s (%d)",
              get_namespace_name(RelationGetNamespace(relation)),
              RelationGetRelationName(relation),
              get_tablespace_name(newrlocator->spcOid), newrlocator->spcOid);

  tupdesc = relation->rd_att;
  for (int i = 0; i < tupdesc->natts; ++i)
    ArrowArrayGet(relation->rd_id, &tupdesc->attrs[i],
                  O_RDWR | O_CREAT | O_EXCL);

  DEBUG_LEAVE("relation: %s.%s",
              get_namespace_name(RelationGetNamespace(relation)),
              RelationGetRelationName(relation));
}

static void arrowam_relation_nontransactional_truncate(Relation relation) {}

static void arrowam_copy_data(Relation relation,
                              const RelFileLocator *newrlocator) {}

static void arrowam_copy_for_cluster(Relation OldTable, Relation NewTable,
                                     Relation OldIndex, bool use_sort,
                                     TransactionId OldestXmin,
                                     TransactionId *xid_cutoff,
                                     MultiXactId *multi_cutoff,
                                     double *num_tuples, double *tups_vacuumed,
                                     double *tups_recently_dead) {}

static void arrowam_vacuum(Relation relation, VacuumParams *params,
                           BufferAccessStrategy bstrategy) {}

static bool arrowam_scan_analyze_next_block(TableScanDesc scan,
                                            BlockNumber blockno,
                                            BufferAccessStrategy bstrategy) {
  return false;
}

static bool arrowam_scan_analyze_next_tuple(TableScanDesc scan,
                                            TransactionId OldestXmin,
                                            double *liverows, double *deadrows,
                                            TupleTableSlot *slot) {
  return false;
}

static double arrowam_index_build_range_scan(
    Relation tableRelation, Relation indexRelation, IndexInfo *indexInfo,
    bool allow_sync, bool anyvisible, bool progress, BlockNumber start_blockno,
    BlockNumber numblocks, IndexBuildCallback callback, void *callback_state,
    TableScanDesc scan) {
  return 0;
}

static void arrowam_index_validate_scan(Relation tableRelation,
                                        Relation indexRelation,
                                        IndexInfo *indexInfo, Snapshot snapshot,
                                        ValidateIndexState *state) {}

static uint64 arrowam_relation_size(Relation relation, ForkNumber forkNumber) {
  return 0;
}

static bool arrowam_relation_needs_toast_table(Relation relation) {
  return false;
}

static void arrowam_estimate_rel_size(Relation relation, int32 *attr_widths,
                                      BlockNumber *pages, double *tuples,
                                      double *allvisfrac) {
  *attr_widths = 0;
  *tuples = 0;
  *allvisfrac = 0;
  *pages = 0;
}

static bool arrowam_scan_bitmap_next_block(TableScanDesc scan,
                                           TBMIterateResult *tbmres) {
  return false;
}

static bool arrowam_scan_bitmap_next_tuple(TableScanDesc scan,
                                           TBMIterateResult *tbmres,
                                           TupleTableSlot *slot) {
  return false;
}

static bool arrowam_scan_sample_next_block(TableScanDesc scan,
                                           SampleScanState *scanstate) {
  return false;
}

static bool arrowam_scan_sample_next_tuple(TableScanDesc scan,
                                           SampleScanState *scanstate,
                                           TupleTableSlot *slot) {
  return false;
}

static const TableAmRoutine arrowam_methods = {
    .type = T_TableAmRoutine,

    .slot_callbacks = arrowam_slot_callbacks,

    .scan_begin = arrowam_scan_begin,
    .scan_end = arrowam_scan_end,
    .scan_rescan = arrowam_scan_rescan,
    .scan_getnextslot = arrowam_scan_getnextslot,

    .parallelscan_estimate = table_block_parallelscan_estimate,
    .parallelscan_initialize = table_block_parallelscan_initialize,
    .parallelscan_reinitialize = table_block_parallelscan_reinitialize,

    .index_fetch_begin = arrowam_index_fetch_begin,
    .index_fetch_reset = arrowam_index_fetch_reset,
    .index_fetch_end = arrowam_index_fetch_end,
    .index_fetch_tuple = arrowam_index_fetch_tuple,

    .tuple_insert = arrowam_tuple_insert,
    .tuple_insert_speculative = arrowam_tuple_insert_speculative,
    .tuple_complete_speculative = arrowam_tuple_complete_speculative,
    .multi_insert = arrowam_multi_insert,
    .tuple_delete = arrowam_tuple_delete,
    .tuple_update = arrowam_tuple_update,
    .tuple_lock = arrowam_tuple_lock,
    .finish_bulk_insert = arrowam_finish_bulk_insert,

    .tuple_fetch_row_version = arrowam_fetch_row_version,
    .tuple_get_latest_tid = arrowam_get_latest_tid,
    .tuple_tid_valid = arrowam_tuple_tid_valid,
    .tuple_satisfies_snapshot = arrowam_tuple_satisfies_snapshot,
    .index_delete_tuples = arrowam_index_delete_tuples,

    .relation_set_new_filelocator = arrowam_relation_set_new_filelocator,
    .relation_nontransactional_truncate =
        arrowam_relation_nontransactional_truncate,
    .relation_copy_data = arrowam_copy_data,
    .relation_copy_for_cluster = arrowam_copy_for_cluster,
    .relation_vacuum = arrowam_vacuum,
    .scan_analyze_next_block = arrowam_scan_analyze_next_block,
    .scan_analyze_next_tuple = arrowam_scan_analyze_next_tuple,
    .index_build_range_scan = arrowam_index_build_range_scan,
    .index_validate_scan = arrowam_index_validate_scan,

    .relation_size = arrowam_relation_size,
    .relation_needs_toast_table = arrowam_relation_needs_toast_table,

    .relation_estimate_size = arrowam_estimate_rel_size,

    .scan_bitmap_next_block = arrowam_scan_bitmap_next_block,
    .scan_bitmap_next_tuple = arrowam_scan_bitmap_next_tuple,
    .scan_sample_next_block = arrowam_scan_sample_next_block,
    .scan_sample_next_tuple = arrowam_scan_sample_next_tuple,
};

Datum arrowam_handler(PG_FUNCTION_ARGS) { PG_RETURN_POINTER(&arrowam_methods); }

/*
 * The function _PG_init gets called with the database id set in
 * variable MyDatabaseId if you load a function from it. If loaded
 * using preload flags, it will be 0.
 */
void _PG_init(void) { ArrowPageSize = sysconf(_SC_PAGESIZE); }
