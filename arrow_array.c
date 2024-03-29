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
 * ArrowArray management module.
 *
 * This module makes sure to create ArrowArray structures that are
 * cached in memory.
 */
#include "arrow_array.h"

#include <postgres.h>

#include <catalog/pg_attribute.h>
#include <miscadmin.h>
#include <utils/hsearch.h>
#include <utils/memutils.h>

#include <sys/mman.h>
#include <sys/stat.h>

#include "debug.h"

typedef struct ArrowArrayEntry {
  ArrowSegmentKey key;
  ArrowSegment* segment;
  struct ArrowArray* array;
} ArrowArrayEntry;

typedef struct SegmentData {
  /** Pointer to the length field in associated segment */
  int64* plength;
} SegmentData;

static void ReleaseSegmentData(struct ArrowArray* array) {
  pfree(array->private_data);
}

static void IncreaseLength(struct ArrowArray* array, int incr) {
  SegmentData* data = (SegmentData*)array->private_data;
  array->length += incr;
  *data->plength += incr;
}

static bool ArrowArrayIsNull(ArrowArray* array, int64 index) {
  int8* ptr = array->buffers[0];
  Assert(index < array->length);
  return ptr[index / 8] & (1 << (index % 8));
}

#define MAKE_ARRAY_GETTER(PFX, TYPE)                                      \
  static NullableDatum ArrowArrayGet##PFX(ArrowArray* array, int index) { \
    TYPE* ptr = array->buffers[1];                                        \
    NullableDatum result = {0};                                           \
    if (ArrowArrayIsNull(array, index))                                   \
      result.isnull = true;                                               \
    else                                                                  \
      result.value = PFX##GetDatum(ptr[index]);                           \
    return result;                                                        \
  }

MAKE_ARRAY_GETTER(Int16, int16);
MAKE_ARRAY_GETTER(Int32, int32);
MAKE_ARRAY_GETTER(Int64, int64);
MAKE_ARRAY_GETTER(Float4, float4);
MAKE_ARRAY_GETTER(Float8, float8);

#define MAKE_ARRAY_APPENDER(PFX, TYPE)                                \
  static void ArrowArrayAppend##PFX(ArrowArray* array, Datum datum) { \
    TYPE* ptr = array->buffers[1];                                    \
    ptr[array->length] = DatumGet##PFX(datum);                        \
    IncreaseLength(array, 1);                                         \
  }

MAKE_ARRAY_APPENDER(Float4, float4);
MAKE_ARRAY_APPENDER(Float8, float8);
MAKE_ARRAY_APPENDER(Int16, int16);
MAKE_ARRAY_APPENDER(Int32, int32);
MAKE_ARRAY_APPENDER(Int64, int64);

static HTAB* ArrowArrayCache;
static MemoryContext ArrowArrayCacheMemoryContext;

static void CreateArrowArrayHash() {
  HASHCTL ctl;

  ArrowArrayCacheMemoryContext = AllocSetContextCreate(
      CacheMemoryContext, "Arrow array cache memory context",
      ALLOCSET_DEFAULT_SIZES);

  ctl.keysize = sizeof(ArrowSegmentKey);
  ctl.entrysize = sizeof(ArrowArrayEntry);
  ctl.hcxt = ArrowArrayCacheMemoryContext;
  ArrowArrayCache = hash_create("Arrow array cache", 400, &ctl,
                                HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

void ArrowArrayAppendNull(ArrowArray* array) {
  int8* ptr = array->buffers[0];
  DEBUG_ENTER("length: %lu", array->length);
  ptr[array->length / 8] |= 1 << (array->length % 8);
  IncreaseLength(array, 1);
  DEBUG_LEAVE("length: %lu", array->length);
}

void ArrowArrayAppendDatum(ArrowArray* array, Form_pg_attribute attr,
                           Datum datum) {
  DEBUG_ENTER("length: %lu, attr: %s", array->length, NameStr(attr->attname));

  switch (attr->atttypid) {
    case INT8OID:
      ArrowArrayAppendInt64(array, datum);
      break;

    case INT4OID:
      ArrowArrayAppendInt32(array, datum);
      break;

    case INT2OID:
      ArrowArrayAppendInt16(array, datum);
      break;

    case FLOAT4OID:
      ArrowArrayAppendFloat4(array, datum);
      break;

    case FLOAT8OID:
      ArrowArrayAppendFloat8(array, datum);
      break;
  }
  DEBUG_LEAVE("length: %lu", array->length);
}

/**
 * Initialize a new arrow array from an arrow segment.
 *
 * This sets all pointers correctly and allows arrow functions to use
 * the arrow array as usual.
 */
ArrowArray* ArrowArrayInit(ArrowSegment* segment, Form_pg_attribute attr,
                           MemoryContext cxt) {
  void* offset_buffer = (int8_t*)segment + segment->offset_buffer_offset;
  void* data_buffer = (int8_t*)segment + segment->data_buffer_offset;
  void* validity_buffer = (int8_t*)segment + segment->validity_buffer_offset;
  MemoryContext oldcontext = MemoryContextSwitchTo(cxt);
  ArrowArray* array = palloc0(sizeof(ArrowArray));
  SegmentData* data = palloc0(sizeof(SegmentData));

  data->plength = &segment->length;

  array->n_buffers = attr->attlen > 0 ? 2 : 3;
  array->buffers = palloc0(array->n_buffers * sizeof(*array->buffers));
  array->null_count = -1;
  array->private_data = data;
  array->release = ReleaseSegmentData;
  array->length = segment->length;

  if (attr->attlen > 0) {
    /* Primitive Layout */
    array->buffers[0] = validity_buffer;
    array->buffers[1] = data_buffer;
  } else {
    /* Variable Binary Layout */
    array->buffers[0] = validity_buffer;
    array->buffers[1] = offset_buffer;
    array->buffers[2] = data_buffer;
  }

  MemoryContextSwitchTo(oldcontext);

  return array;
}

void ArrowArrayRelease(ArrowArray* array) {
  (*array->release)(array);
  array->release = NULL; /* Just for precausion */
  pfree(array);
}

NullableDatum ArrowArrayGetDatum(ArrowArray* array, Form_pg_attribute attr,
                                 int index) {
  switch (attr->atttypid) {
    case INT8OID:
      return ArrowArrayGetInt64(array, index);

    case INT4OID:
      return ArrowArrayGetInt32(array, index);

    case INT2OID:
      return ArrowArrayGetInt16(array, index);

    case FLOAT4OID:
      return ArrowArrayGetFloat4(array, index);

    case FLOAT8OID:
      return ArrowArrayGetFloat8(array, index);

    default:
      elog(ERROR, "type %d for attribute %s not handled", attr->atttypid,
           NameStr(attr->attname));
  }
}

/*
 * Map an existing block into memory and save pointers to it in cache.
 *
 * Optionally create the segment if it does not exist.
 */
ArrowArray* ArrowArrayGet(Oid reloid, Form_pg_attribute attr, int oflags) {
  const ArrowSegmentKey key = {
      .bk_dbid = MyDatabaseId,
      .bk_relid = reloid,
      .bk_attno = attr->attnum,
  };
  bool found;
  ArrowArrayEntry* entry;

  DEBUG_ENTER("relid: %d, attr: %s", reloid, NameStr(attr->attname));

  if (ArrowArrayCache == NULL)
    CreateArrowArrayHash();

  entry = hash_search(ArrowArrayCache, &key, HASH_FIND, &found);
  if (!found) {
    bool created;
    //    const int oflags = create ? (O_RDWR | O_CREAT) : O_RDWR;
    ArrowSegment* segment = ArrowSegmentOpen(&key, oflags, 0644, &created);
    if (created)
      ArrowSegmentInit(segment, attr);
    entry = hash_search(ArrowArrayCache, &key, HASH_ENTER, NULL);
    entry->segment = segment;
    entry->array =
        ArrowArrayInit(entry->segment, attr, ArrowArrayCacheMemoryContext);
  }

  DEBUG_LEAVE("address: %p", entry->array);
  return entry->array;
}
