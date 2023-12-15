MODULE_big = arrow
OBJS = arrowam_handler.o arrow_tts.o debug.o arrow_storage.o arrow_array.o

EXTENSION = arrow
DATA = arrow--0.1.sql
PGFILEDESC = "arrow - in-memory columnar store"

REGRESS = basic

PG_CPPFLAGS = -DAM_TRACE=1

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

arrowam_handler.o: arrowam_handler.c arrowam_handler.h arrow_array.h	\
 arrow_c_data_interface.h arrow_storage.h arrow_scan.h arrow_tts.h	\
 debug.h
arrow_array.o: arrow_array.c arrow_array.h arrow_c_data_interface.h	\
 arrow_storage.h debug.h
arrow_storage.o: arrow_storage.c arrow_storage.h	\
 arrow_c_data_interface.h arrow_tts.h debug.h
arrow_tts.o: arrow_tts.c arrow_tts.h arrow_c_data_interface.h	\
 arrow_array.h arrow_storage.h debug.h
debug.o: debug.c debug.h arrow_storage.h arrow_c_data_interface.h
