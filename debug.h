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

#ifndef DEBUG_H_
#define DEBUG_H_

#include <postgres.h>

#include <executor/tuptable.h>
#include <lib/stringinfo.h>
#include <utils/elog.h>

#include "arrow_storage.h"

#ifdef AM_TRACE
#define DEBUG_ENTER(FMT, ...)                              \
  do {                                                     \
    elog(DEBUG2, ">>> %s: " FMT, __func__, ##__VA_ARGS__); \
  } while (0)
#define DEBUG_LEAVE(FMT, ...)                              \
  do {                                                     \
    elog(DEBUG2, "<<< %s: " FMT, __func__, ##__VA_ARGS__); \
  } while (0)
#define DEBUG_LOG(FMT, ...)                                \
  do {                                                     \
    elog(DEBUG2, "--- %s: " FMT, __func__, ##__VA_ARGS__); \
  } while (0)
#else
#define DEBUG_ENTER(FMT, ...)
#define DEBUG_LEAVE(FMT, ...)
#define DEBUG_LOG(FMT, ...)
#endif

#define YESNO(V) ((V) ? "yes" : "no")

extern StringInfo show_slot(TupleTableSlot* slot);
extern StringInfo key_to_string(const ArrowSegmentKey* key);

#endif /* DEBUG_H_ */
