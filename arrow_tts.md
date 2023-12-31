# Arrow C Data Interface

We use the [Arrow C Data Interface][1] as the in-memory format for the
access method. If you're interested of the rationale and details about
the format, please look at the [Apache Arrow][3] page.

In this description you will see an explaination for how the the
`ArrowArray` structures are used for the implementation of the
`arrowam_handler` and the `ArrowTupleTableSlot`.

Note that we do not use, nor need to use `ArrowSchema` at all since
Postgres manages the table structure and contains all the necessary
information.

## The `ArrowArray` structure

The `ArrowArray` structure contains information about a single column
of data and references to buffers containing the actual data. The
definition is the following:

```c
typedef struct ArrowArray {
  int64 length;
  int64 null_count;
  int64 offset;
  int64 n_buffers;
  int64 n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;

  void (*release)(struct ArrowArray*);
  void* private_data;
} ArrowArray;
```

The buffers have slightly different usage depending on the type of the
column.

| Layout Type          | Buffer 0 | Buffer 1       | Buffer 2 | Variadic Buffers |
|----------------------|----------|----------------|----------|------------------|
| Primitive            | validity | data           |          |                  |
| Variable Binary      | validity | offsets        | data     |                  |
| Variable Binary View | validity | views          |          | data             |
| List                 | validity | offsets        |          |                  |
| Fixed-size List      | validity |                |          |                  |
| Struct               | validity |                |          |                  |
| Sparse Union         | type ids |                |          |                  |
| Dense Union          | type ids | offsets        |          |                  |
| Null                 |          |                |          |                  |
| Dictionary-encoded   | validity | data (indices) |          |                  |
| Run-end encoded      |          |                |          |                  |

We only use the first two types: the "Primitive" and the "Variable
Binary" layouts. For this reason, the implementation is not using the
children arrays, nor the dictionary.

## Shared Memory Storage

Since the `ArrowArray` structure contains pointers to memory, we need
to have a separate representation of the data in the shared memory
storage, which is named `ArrowSegment`.

There are a few reasons to why we need a separate structure for the
data stored in the memory segment:

1. Raw pointers cannot be used in shared memory segments since the
   start of the shared memory segment will be mapped to different
   addresses in different processes. Instead we store offsets to the
   buffers in the shared memory segment.
2. We are using a separate `ArrowArray` structure for the in-memory
   storage that is local to each process. This allows us to use normal
   Arrow libraries for accessing the data.
3. The length in the `ArrowArray` structure need to be updated at the
   same time as the segment length, and `ArrowArray` length cannot
   point to another length that is being updated at the same
   time. Indirectly, each `ArrowArray` instance is assumed to be the
   sole owner of the data.

The storage of the data is otherwise similar to the `ArrowArray`
structure:

- The validity bitmap is stored in the same way.
- The data buffer is stored in the same way.
- The offset buffer is stored in the same way.

## Shared Memory Naming

The table access method relies on using shared memory *only*, that is,
there are no WAL writes, and no backing disk storage for the in-memory
access at all (except those that are part of the virtual memory
implementation in Linux).

The shared memory blocks are named `arrow.<dbid>.<relid>.<attno>` and
each block contains the `ArrowSegment` headers structure and all the
buffers for the array. Since we are only using the two first formats,
we

    +------------------------+
    |    ArrowArray header   |
    +------------------------+
    |         buffer 1       |
    +------------------------+
    |         buffer 0       |
    |  (validity bitmapset)  |
    +------------------------+
  	
We place buffer 0 last in the block because it is smaller, so when
resizing, there is less data to move.

[1]: https://arrow.apache.org/docs/format/CDataInterface.html
[2]: https://arrow.apache.org/docs/format/Columnar.html
[3]: https://arrow.apache.org/docs/index.html
