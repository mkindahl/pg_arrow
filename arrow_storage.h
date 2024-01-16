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

/**
 * Module for primitives handling shared blocks.
 *
 * Blocks are allocated based on database OID, the table OID, and the
 * attribute id. The shared memory block is resized as needed using
 * mremap(2), so it is limited to Linux.
 */

#ifndef ARROW_STORAGE_H_
#define ARROW_STORAGE_H_

#include <postgres.h>

#include <limits.h>
#include <semaphore.h>

#include "arrow_c_data_interface.h"

#include <utils/rel.h>

/**
 * Key for arrow arrays.
 *
 * Each ArrowArray is stored in a separate (named) shared memory
 * segment with database, relation, and attribute used as part of the
 * name.
 */
typedef struct ArrowSegmentKey {
  Oid bk_dbid;    /* Database OID */
  Oid bk_relid;   /* Relation OID */
  int16 bk_attno; /* Attribute number */
} ArrowSegmentKey;

/**
 * Column array inspired by the Apache Arrow specification, but with
 * some tweaks to support a shared memory implementation.
 *
 * It is intended to allow the ArrowArray buffers to be mapped
 * directly into each segment.
 *
 * In particular, we do not store pointers in this structure and
 * rather offsets relative the start of the arrow segment.
 *
 * For now, we store the offsets explicitly named, but we might well store all
 * buffer offsets later and mimic the structure of the ArrowArray structure, but
 * using offsets relative start of segment instead.
 */
typedef struct ArrowSegment {
  /** Length of the array, in number of elements */
  int64 length;

  /** Attribute length, same as for PostgreSQL */
  int16 attlen;

  /** Offset to validity buffer relative to start of segment. */
  size_t validity_buffer_offset;

  /** Offset to buffer for data, either fixed-size or variable size,
   * relative to start of segment. */
  size_t data_buffer_offset;

  /** Offset to buffer for offsets used for variable length data
   *  relative to start of segment if using variable length data,
   *  otherwise 0 */
  size_t offset_buffer_offset;
} ArrowSegment;

extern size_t ArrowPageSize;

ArrowSegment* ArrowSegmentOpen(const ArrowSegmentKey* key, int oflag,
                               mode_t mode, bool* created);
bool ArrowSegmentExists(const ArrowSegmentKey* key);
void ArrowSegmentInit(ArrowSegment* segment, Form_pg_attribute attr);

#endif /* ARROW_STORAGE_H_*/
