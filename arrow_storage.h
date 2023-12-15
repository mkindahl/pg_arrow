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

#include <utils/rel.h>

#include <limits.h>
#include <semaphore.h>

#include "arrow_c_data_interface.h"

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
 */
typedef struct ArrowSegment {
  /** Length of the array, in number of elements */
  int64 length;

  /** Attribute length, same as for PostgreSQL */
  int16 attlen;

  /** Offset to validity buffer relative to start of segment. */
  size_t validity_buffer_offset;

  /** Offset to buffer for data, either fixed-size or variable size, relative to
   * start of segment. */
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
