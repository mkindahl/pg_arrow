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

#ifndef ARROW_TTS_H_
#define ARROW_TTS_H_

#include <postgres.h>

#include <access/tupdesc.h>
#include <executor/tuptable.h>
#include <utils/rel.h>

#include "arrow_c_data_interface.h"

/**
 * Arrow Tuple Table Slot.
 *
 * The Arrow TTS contains an array of pointers to shared memory
 * buffers as well as the index of the entry in the arrays that is
 * current.
 *
 * In many respects, it is similar in functionality to RecordBatch
 * from the Apache Arrow library, but we use the tuple descriptor as
 * the schema.
 *
 * The index cannot be negative, but since arrow array offsets are
 * signed, we stick to the same convention for the indexes. It will
 * allow us to encode additional information using negative numbers.
 *
 * The length of the array is copied from the ArrowArray columns. They
 * should all have the same length, which is the logical length of the
 * arrays, which is the same as the number of rows.
 */
typedef struct ArrowTupleTableSlot {
  TupleTableSlot base;
  int64 index;
#if 0
  int64 length; /* Copied from the arrays */
#endif
  ArrowArray **columns;
} ArrowTupleTableSlot;

extern PGDLLIMPORT const TupleTableSlotOps TTSOpsArrowTuple;

#define TTS_IS_ARROWTUPLE(SLOT) ((slot)->tts_ops == &TTSOpsArrowTuple)

TupleTableSlot *ExecStoreArrowTuple(TupleTableSlot *slot);
void ExecInsertArrowSlot(Relation relation, Oid relid, TupleTableSlot *slot,
                         CommandId cid, int options);
#endif
