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

#include "debug.h"

#include <postgres.h>
#include <fmgr.h>

#include <executor/tuptable.h>
#include <lib/stringinfo.h>
#include <utils/lsyscache.h>

/* Call the output function for a value given the type and append the string
 * representation to the provided buffer. */
static void ValueOut(StringInfo buf, Oid typid, Datum value, bool isnull) {
  Oid typoutputfunc;
  bool typIsVarlena;
  FmgrInfo typoutputfinfo;

  getTypeOutputInfo(typid, &typoutputfunc, &typIsVarlena);
  fmgr_info(typoutputfunc, &typoutputfinfo);
  if (isnull)
    appendStringInfoString(buf, "NULL");
  else
    appendStringInfoString(buf, OutputFunctionCall(&typoutputfinfo, value));
}

StringInfo show_slot(TupleTableSlot* slot) {
  StringInfo info = makeStringInfo();
  int natt = 0;

  appendStringInfoString(info, "(");
  while (true) {
    const Form_pg_attribute att =
        TupleDescAttr(slot->tts_tupleDescriptor, natt);

    ValueOut(info, att->atttypid, slot->tts_values[natt],
             slot->tts_isnull[natt]);

    if (++natt < slot->tts_tupleDescriptor->natts)
      appendStringInfoString(info, ", ");
    else
      break;
  }
  appendStringInfoString(info, ")");
  return info;
}

StringInfo key_to_string(const ArrowSegmentKey* key) {
  StringInfo info = makeStringInfo();
  appendStringInfo(info, "(%u, %u, %u)", key->bk_dbid, key->bk_relid,
                   key->bk_attno);
  return info;
}
