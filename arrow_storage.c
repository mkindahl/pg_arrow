/**
 * Module for managing the memory segments for the storage.
 *
 * Each column of the table is stored in a separate (named) shared
 * memory segment and we keep a cache with pointers to memory segments
 * based on a ArrowArray key. Each memory segment is protected by a
 * semaphore to ensure that there is a single writer at each time.
 */

#include "arrow_storage.h"

#include <postgres.h>

#include <utils/catcache.h>

#include <fcntl.h> /* For O_* constants */
#include <limits.h>
#include <sys/mman.h>
#include <sys/stat.h> /* For mode constants */
#include <sys/types.h>
#include <unistd.h>

#include "arrow_tts.h"
#include "debug.h"

size_t ArrowPageSize;

void ArrowSegmentInit(ArrowSegment* segment, Form_pg_attribute attr) {
  const size_t bytes = (ArrowPageSize - sizeof(ArrowSegment)) / 8;
  const size_t validity_buffer_size = 64 * (bytes / 64 + 1);

  memset(segment, 0, sizeof(*segment));

  segment->attlen = attr->attlen;
  segment->validity_buffer_offset = ArrowPageSize - validity_buffer_size;
  segment->data_buffer_offset = sizeof(*segment);
  /* offset_buffer_offset not yet used */
}

static void ArrowBuildPath(const ArrowSegmentKey* key, char* path,
                           size_t path_size) {
  size_t count = snprintf(path, path_size, "/arrow.%u.%u.%u", key->bk_dbid,
                          key->bk_relid, key->bk_attno);
  if (count >= path_size)
    ereport(ERROR, (errcode(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION)),
                    errmsg("buffer not large enough for shared buffer name"),
                    errdetail("expected %lu bytes, but was %lu", count,
                              path_size - 1)));
}

/*
 * Open an (arrow array) shared memory block.
 *
 * A shared segment arrow array is opened using the oflags and
 * mode. The resulting path is stored in `path`, which points to a
 * buffer of size `size`.
 */
ArrowSegment* ArrowSegmentOpen(const ArrowSegmentKey* key, int oflag,
                               mode_t mode, bool* created) {
  char path[256];
  int fd;
  struct stat sb;
  ArrowSegment* segment;
  DEBUG_ENTER("key: %s", key_to_string(key)->data);

  ArrowBuildPath(key, path, sizeof(path));
  fd = shm_open(path, oflag, mode);
  if (fd < 0)
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not open path \"%s\": %m", path)));

  if (fstat(fd, &sb) == -1)
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("unable to stat file \"%s\": %m", path)));
  if (sb.st_size == 0) {
    if (ftruncate(fd, ArrowPageSize) != 0)
      ereport(ERROR, (errcode_for_file_access(),
                      errmsg("could not truncate file \"%s\" to %lu: %m", path,
                             ArrowPageSize)));
    if (created)
      *created = true;
  } else if (created) {
    *created = false;
  }

  segment = mmap(NULL, sb.st_size == 0 ? ArrowPageSize : sb.st_size,
                 PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  close(fd);

  DEBUG_LEAVE("path %s", path);
  return segment;
}

bool ArrowSegmentExists(const ArrowSegmentKey* key) {
  char path[256];
  int fd;

  ArrowBuildPath(key, path, sizeof(path));
  fd = shm_open(path, O_RDONLY, 0644);
  if (fd < 0 && errno == ENOENT)
    return false;
  if (fd < 0)
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not open path \"%s\": %m", path)));
  close(fd);
  return true;
}
