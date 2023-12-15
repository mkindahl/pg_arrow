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
