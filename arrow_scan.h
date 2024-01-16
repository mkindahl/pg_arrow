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

#ifndef ARROW_SCAN_H_
#define ARROW_SCAN_H_

#include <postgres.h>

#include <access/heapam.h>
#include <executor/tuptable.h>
#include <utils/relcache.h>

#include "arrow_storage.h"
#include "arrow_tts.h"

/**
 * Arrow array scan descriptor.
 */
typedef struct ArrowScanDesc {
  TableScanDescData base;
  int64 index;
  int64 length;
} ArrowScanDesc;

#endif /* ARROW_SCAN_H_*/
