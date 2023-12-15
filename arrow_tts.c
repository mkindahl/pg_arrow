#include "arrow_tts.h"

#include <postgres.h>

#include <executor/tuptable.h>
#include <miscadmin.h>

#include "arrow_array.h"
#include "debug.h"

static void tts_arrow_init(TupleTableSlot *slot) {
  ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *)slot;
  int natts = slot->tts_tupleDescriptor->natts;

  aslot->index = 0;
  aslot->columns = palloc0(natts * sizeof(*aslot->columns));
}

static void tts_arrow_release(TupleTableSlot *slot) {
  ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *)slot;
  for (int i = 0; i < aslot->base.tts_nvalid; ++i)
    ArrowArrayRelease(aslot->columns[i]);
  pfree(aslot->columns);
}

/**
 * Clear the Arrow TTS.
 *
 * Clearing the Arrow TTS will just clear the data and isnull arrays
 * as well as marking the slot as empty. It will not remove the
 * reference to the associated arrow arrays
 */
static void tts_arrow_clear(TupleTableSlot *slot) {
  slot->tts_nvalid = 0;
  slot->tts_flags |= TTS_FLAG_EMPTY;
  ItemPointerSetInvalid(&slot->tts_tid);
}

static void tts_arrow_materialize(TupleTableSlot *slot) {}

static void tts_arrow_copyslot(TupleTableSlot *dstslot,
                               TupleTableSlot *srcslot) {
  TupleDesc srcdesc = srcslot->tts_tupleDescriptor;
  DEBUG_ENTER("srcslot: %s", show_slot(srcslot)->data);

  Assert(srcdesc->natts <= dstslot->tts_tupleDescriptor->natts);

  ExecClearTuple(dstslot);

  slot_getallattrs(srcslot);

  for (int natt = 0; natt < srcdesc->natts; natt++) {
    dstslot->tts_values[natt] = srcslot->tts_values[natt];
    dstslot->tts_isnull[natt] = srcslot->tts_isnull[natt];
  }

  dstslot->tts_nvalid = srcdesc->natts;
  dstslot->tts_flags &= ~TTS_FLAG_EMPTY;

  /* TTSOpsVirtualTuple has this, not entirely sure if it is
     needed. Comment is "make sure storage doesn't depend on external
     memory." */
  tts_arrow_materialize(dstslot);
  DEBUG_LEAVE("dstslot: %s", show_slot(dstslot)->data);
}

static Datum tts_arrow_getsysattr(TupleTableSlot *slot, int attnum,
                                  bool *isnull) {
  DEBUG_ENTER("slot: %s, attnum: %d, isnull: %d", show_slot(slot)->data, attnum,
              *isnull);
  DEBUG_LEAVE("");
  return 0; /* silence compiler warnings */
}

static void tts_arrow_getsomeattrs(TupleTableSlot *slot, int natts) {
  TupleDesc tupdesc = slot->tts_tupleDescriptor;
  ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *)slot;

  DEBUG_ENTER("slot.tts_tableOid=%d, slot.nvalid=%d, natts=%d",
              slot->tts_tableOid, slot->tts_nvalid, natts);

  ExecClearTuple(slot);

  /* Fetch missing columns */
  while (slot->tts_nvalid < natts) {
    Form_pg_attribute attr = TupleDescAttr(tupdesc, slot->tts_nvalid);
    if (aslot->columns[slot->tts_nvalid] == NULL)
      aslot->columns[slot->tts_nvalid] =
          ArrowArrayGet(slot->tts_tableOid, attr, O_RDWR);
    ++slot->tts_nvalid;
  }

  ExecStoreArrowTuple(slot);

  DEBUG_LEAVE("slot.nvalid=%d", slot->tts_nvalid);
}

static HeapTuple tts_arrow_copy_heap_tuple(TupleTableSlot *slot) {
  Assert(!TTS_EMPTY(slot));

  return heap_form_tuple(slot->tts_tupleDescriptor, slot->tts_values,
                         slot->tts_isnull);
}

static MinimalTuple tts_arrow_copy_minimal_tuple(TupleTableSlot *slot) {
  Assert(!TTS_EMPTY(slot));

  return heap_form_minimal_tuple(slot->tts_tupleDescriptor, slot->tts_values,
                                 slot->tts_isnull);
}

/**
 * Store arrow tuple into slot.
 *
 * An Arrow tuple here is actually a subset of the columns for the
 * table. This function is rather used to fill in the datum and isnull
 * arrays and mark the TTS as filled and should be used as follows:
 *
 * - Call ExecClearTuple to mark it as clear. This will just remove
 *   the isnull and datum arrays, not the arrow array columns nor
 *   change the  * - Make sure that the ArrowArray is properly set up and the
 * index is correctly set.
 *
 * - Call this function to fill in the datum array and isnull array
 *   based on the arrow arrays and the index.
 *
 * Note that we could, similar to how the virtual tuple works, release
 * the arrow arrays if they are owned by the tuple, but since these
 * are allocated in shared memory, we can not "release" them by
 * setting the array to zero and drop the arrow array.
 */
TupleTableSlot *ExecStoreArrowTuple(TupleTableSlot *slot) {
  ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *)slot;
  Assert(slot != NULL);
  Assert(slot->tts_tupleDescriptor != NULL);
  Assert(TTS_EMPTY(slot));

  if (unlikely(!TTS_IS_ARROWTUPLE(slot)))
    elog(ERROR, "trying to store an Arrow array into wrong type of slot");

  for (int i = 0; i < slot->tts_nvalid; ++i) {
    Form_pg_attribute attr = TupleDescAttr(slot->tts_tupleDescriptor, i);
    if (aslot->columns[i] != NULL) {
      NullableDatum datum =
          ArrowArrayGetDatum(aslot->columns[i], attr, aslot->index);
      slot->tts_values[i] = datum.value;
      slot->tts_isnull[i] = datum.isnull;
    }
  }

  slot->tts_flags &= ~TTS_FLAG_EMPTY;

  return slot;
}

/**
 * Insert data in a slot into the corresponding arrow arrays.
 */
void ExecInsertArrowSlot(Relation relation, Oid relid, TupleTableSlot *slot,
                         CommandId cid, int options) {
  ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *)slot;
  TupleDesc tupdesc = slot->tts_tupleDescriptor;

  /* Iterate over all the columns and add the value to each column. */
  for (int i = 0; i < tupdesc->natts; ++i) {
    Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
    aslot->columns[i] = ArrowArrayGet(relid, attr, O_RDWR);
    if (slot->tts_isnull[i])
      ArrowArrayAppendNull(aslot->columns[i]);
    else
      ArrowArrayAppendDatum(aslot->columns[i], attr, slot->tts_values[i]);
  }
}

const TupleTableSlotOps TTSOpsArrowTuple = {
    .base_slot_size = sizeof(ArrowTupleTableSlot),
    .init = tts_arrow_init,
    .release = tts_arrow_release,
    .clear = tts_arrow_clear,
    .getsomeattrs = tts_arrow_getsomeattrs,
    .getsysattr = tts_arrow_getsysattr,
    .materialize = tts_arrow_materialize,
    .copyslot = tts_arrow_copyslot,

    /* A memory tuple table slot can not "own" a heap tuple or a
     * minimal tuple. This will then fall back on the copy method
     * instead. */
    .get_heap_tuple = NULL,
    .get_minimal_tuple = NULL,
    .copy_heap_tuple = tts_arrow_copy_heap_tuple,
    .copy_minimal_tuple = tts_arrow_copy_minimal_tuple,
};
