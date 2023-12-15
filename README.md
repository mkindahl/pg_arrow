# In-memory Table Access Method

This is an in-memory table access method with a columnar
structure. The columnar structure is based on the [Arrow C Data
Interface][1] and we store each column in dedicated shared memory
segments, one for each buffer according to the [Arrow Columnar
Format][2].

[1]: https://arrow.apache.org/docs/format/CDataInterface.html
[2]: https://arrow.apache.org/docs/format/Columnar.html
