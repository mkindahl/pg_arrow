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
