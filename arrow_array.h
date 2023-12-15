#ifndef ARROW_ARRAY_H_
#define ARROW_ARRAY_H_

#include <postgres.h>

#include <executor/tuptable.h>
#include <utils/catcache.h>

#include "arrow_c_data_interface.h"
#include "arrow_storage.h"

ArrowArray* ArrowArrayInit(ArrowSegment* segment, Form_pg_attribute attr,
                           MemoryContext cxt)
    __attribute__((returns_nonnull, warn_unused_result));
void ArrowArrayRelease(ArrowArray* array);
ArrowArray* ArrowArrayGet(Oid reloid, Form_pg_attribute attr, int oflags)
    __attribute__((returns_nonnull));
NullableDatum ArrowArrayGetDatum(ArrowArray* array, Form_pg_attribute attr,
                                 int index);
void ArrowArrayAppendNull(ArrowArray* array);
void ArrowArrayAppendDatum(ArrowArray* array, Form_pg_attribute attr,
                           Datum datum);

#endif /* ARROW_ARRAY_H_ */
