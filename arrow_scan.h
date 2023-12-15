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
