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
