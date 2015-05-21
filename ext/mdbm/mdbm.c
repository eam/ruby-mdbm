#include "ruby.h"
#include <mdbm.h>
#include <errno.h>

#define ERRBUF_LEN 512

VALUE cMdbm;

static void cb_free_mdbm_db(void *p) {
  mdbm_close(p);
}

VALUE method_open(VALUE self, VALUE file, VALUE flags, VALUE mode, VALUE psize, VALUE presize) {
  rb_iv_set(self, "@foo", INT2NUM(77));
  MDBM * db;
  db = mdbm_open(RSTRING_PTR(file), NUM2INT(flags), NUM2INT(mode), NUM2INT(psize), NUM2INT(presize));
  if (!db) {
    rb_raise(rb_eRuntimeError, "unable to open mdbm");
  }
  printf("db: %p\n", db);
  rb_iv_set(self, "@db", Data_Wrap_Struct(cMdbm, 0, cb_free_mdbm_db, db));

  return self;
}

VALUE method_first(VALUE self) {
  MDBM *db;
  MDBM_ITER *iter;
  VALUE retval = Qnil;
  Data_Get_Struct(rb_iv_get(self, "@db"), MDBM, db);
  mdbm_lock(db);
  kvpair pair = mdbm_first_r(db, iter);
  if (!(pair.key.dsize == 0 && pair.val.dsize == 0)) {
    retval = rb_str_new(pair.key.dptr, pair.key.dsize);
  }
  mdbm_unlock(db);
  return retval;
}

VALUE method_keys(VALUE self) {
  MDBM *db;
  MDBM_ITER iter;
  datum key;
  Data_Get_Struct(rb_iv_get(self, "@db"), MDBM, db);
  VALUE ret = rb_ary_new();
  MDBM_ITER_INIT(&iter);
  mdbm_lock(db);
  while (1) {
    key = mdbm_nextkey_r(db, &iter);
    if (key.dsize == 0) {
      break;
    }
    VALUE data = rb_str_new(key.dptr, key.dsize);
    rb_ary_push(ret, data);
  }
  mdbm_unlock(db);

  return ret;
}

VALUE method_fetch(VALUE self, VALUE arg_key) {
  MDBM *db;
  datum key, val;
  MDBM_ITER iter;
  MDBM_ITER_INIT(&iter); 
  VALUE retval = Qnil;
  Data_Get_Struct(rb_iv_get(self, "@db"), MDBM, db);
  StringValue(arg_key);
  key.dptr = RSTRING_PTR(arg_key);
  key.dsize = RSTRING_LEN(arg_key);
  mdbm_lock(db);
  if (mdbm_fetch_r(db, &key, &val, &iter)) {
    /* check errno, raise after unlock */
    perror("method_fetch ERROR: ");
    mdbm_unlock(db);
    return retval;
  }
  retval = rb_str_new(val.dptr, val.dsize);
  mdbm_unlock(db);
  return retval;
}

VALUE method_store(VALUE self, VALUE arg_key, VALUE arg_val, VALUE arg_flags) {
  MDBM *db;
  datum key, val;
  MDBM_ITER iter;
  int result;
  char err_buf[ERRBUF_LEN];
  MDBM_ITER_INIT(&iter); 
  Data_Get_Struct(rb_iv_get(self, "@db"), MDBM, db);
  StringValue(arg_key);
  key.dptr = RSTRING_PTR(arg_key);
  key.dsize = RSTRING_LEN(arg_key);
  StringValue(arg_val);
  val.dptr = RSTRING_PTR(arg_val);
  val.dsize = RSTRING_LEN(arg_val);

  mdbm_lock(db);
  result = mdbm_store_r(db, &key, &val, NUM2INT(arg_flags), &iter);
  mdbm_unlock(db);

  if (result) {
    /* this returns  `store': No such file or directory (RuntimeError) which is confusing.
     * TODO: find/write a better strerror_r() for mdbm calls
     */
    rb_raise(rb_eRuntimeError, strerror_r(errno, err_buf, ERRBUF_LEN));
  }
  return Qnil;
}

VALUE method_close(VALUE self) {
  MDBM *db;
  Data_Get_Struct(rb_iv_get(self, "@db"), MDBM, db);
  mdbm_close(db);
  return Qnil;
}

void Init_mdbm() {
  cMdbm = rb_define_class("Mdbm", rb_cObject);
  rb_define_method(cMdbm, "initialize", method_open, 5);
  rb_define_method(cMdbm, "first", method_first, 0);
  rb_define_method(cMdbm, "keys", method_keys, 0);
  rb_define_method(cMdbm, "fetch", method_fetch, 1);
  rb_define_method(cMdbm, "[]", method_fetch, 1);
  rb_define_method(cMdbm, "store", method_store, 3);
  rb_define_method(cMdbm, "close", method_close, 0);

  rb_define_const(cMdbm, "MDBM_API_VERSION", INT2NUM(MDBM_API_VERSION));
  rb_define_const(cMdbm, "MDBM_LOC_NORMAL", INT2NUM(MDBM_LOC_NORMAL));
  rb_define_const(cMdbm, "MDBM_LOC_ARENA", INT2NUM(MDBM_LOC_ARENA));
  rb_define_const(cMdbm, "MDBM_O_RDONLY", INT2NUM(MDBM_O_RDONLY));
  rb_define_const(cMdbm, "MDBM_O_WRONLY", INT2NUM(MDBM_O_WRONLY));
  rb_define_const(cMdbm, "MDBM_O_RDWR", INT2NUM(MDBM_O_RDWR));
  rb_define_const(cMdbm, "MDBM_O_CREAT", INT2NUM(MDBM_O_CREAT));
  rb_define_const(cMdbm, "MDBM_O_TRUNC", INT2NUM(MDBM_O_TRUNC));
  rb_define_const(cMdbm, "MDBM_O_FSYNC", INT2NUM(MDBM_O_FSYNC));
  rb_define_const(cMdbm, "MDBM_O_ASYNC", INT2NUM(MDBM_O_ASYNC));
  rb_define_const(cMdbm, "MDBM_O_DIRECT", INT2NUM(MDBM_O_DIRECT));
  rb_define_const(cMdbm, "MDBM_NO_DIRTY", INT2NUM(MDBM_NO_DIRTY));
  rb_define_const(cMdbm, "MDBM_SINGLE_ARCH", INT2NUM(MDBM_SINGLE_ARCH));
  rb_define_const(cMdbm, "MDBM_OPEN_WINDOWED", INT2NUM(MDBM_OPEN_WINDOWED));
  rb_define_const(cMdbm, "MDBM_PROTECT", INT2NUM(MDBM_PROTECT));
  rb_define_const(cMdbm, "MDBM_DBSIZE_MB", INT2NUM(MDBM_DBSIZE_MB));
  rb_define_const(cMdbm, "MDBM_STAT_OPERATIONS", INT2NUM(MDBM_STAT_OPERATIONS));
  rb_define_const(cMdbm, "MDBM_LARGE_OBJECTS", INT2NUM(MDBM_LARGE_OBJECTS));
  rb_define_const(cMdbm, "MDBM_PARTITIONED_LOCKS", INT2NUM(MDBM_PARTITIONED_LOCKS));
  rb_define_const(cMdbm, "MDBM_RW_LOCKS", INT2NUM(MDBM_RW_LOCKS));
  rb_define_const(cMdbm, "MDBM_ANY_LOCKS", INT2NUM(MDBM_ANY_LOCKS));
  rb_define_const(cMdbm, "MDBM_CREATE_V3", INT2NUM(MDBM_CREATE_V3));
  rb_define_const(cMdbm, "MDBM_OPEN_NOLOCK", INT2NUM(MDBM_OPEN_NOLOCK));
  rb_define_const(cMdbm, "MDBM_DEMAND_PAGING", INT2NUM(MDBM_DEMAND_PAGING));
  rb_define_const(cMdbm, "MDBM_DBSIZE_MB_OLD", INT2NUM(MDBM_DBSIZE_MB_OLD));
  rb_define_const(cMdbm, "MDBM_COPY_LOCK_ALL", INT2NUM(MDBM_COPY_LOCK_ALL));
  rb_define_const(cMdbm, "MDBM_SAVE_COMPRESS_TREE", INT2NUM(MDBM_SAVE_COMPRESS_TREE));
  rb_define_const(cMdbm, "MDBM_ALIGN_8_BITS", INT2NUM(MDBM_ALIGN_8_BITS));
  rb_define_const(cMdbm, "MDBM_ALIGN_16_BITS", INT2NUM(MDBM_ALIGN_16_BITS));
  rb_define_const(cMdbm, "MDBM_ALIGN_32_BITS", INT2NUM(MDBM_ALIGN_32_BITS));
  rb_define_const(cMdbm, "MDBM_ALIGN_64_BITS", INT2NUM(MDBM_ALIGN_64_BITS));
/*
  rb_define_const(cMdbm, "_MDBM_MAGIC", INT2NUM(_MDBM_MAGIC));
  rb_define_const(cMdbm, "_MDBM_MAGIC_NEW", INT2NUM(_MDBM_MAGIC_NEW));
  rb_define_const(cMdbm, "_MDBM_MAGIC_NEW2", INT2NUM(_MDBM_MAGIC_NEW2));
*/
  rb_define_const(cMdbm, "MDBM_FETCH_FLAG_DIRTY", INT2NUM(MDBM_FETCH_FLAG_DIRTY));
  rb_define_const(cMdbm, "MDBM_INSERT", INT2NUM(MDBM_INSERT));
  rb_define_const(cMdbm, "MDBM_REPLACE", INT2NUM(MDBM_REPLACE));
  rb_define_const(cMdbm, "MDBM_INSERT_DUP", INT2NUM(MDBM_INSERT_DUP));
  rb_define_const(cMdbm, "MDBM_MODIFY", INT2NUM(MDBM_MODIFY));
  rb_define_const(cMdbm, "MDBM_STORE_MASK", INT2NUM(MDBM_STORE_MASK));
  rb_define_const(cMdbm, "MDBM_RESERVE", INT2NUM(MDBM_RESERVE));
  rb_define_const(cMdbm, "MDBM_CLEAN", INT2NUM(MDBM_CLEAN));
  rb_define_const(cMdbm, "MDBM_CACHE_ONLY", INT2NUM(MDBM_CACHE_ONLY));
  rb_define_const(cMdbm, "MDBM_CACHE_REPLACE", INT2NUM(MDBM_CACHE_REPLACE));
  rb_define_const(cMdbm, "MDBM_CACHE_MODIFY", INT2NUM(MDBM_CACHE_MODIFY));
  rb_define_const(cMdbm, "MDBM_STORE_SUCCESS", INT2NUM(MDBM_STORE_SUCCESS));
  rb_define_const(cMdbm, "MDBM_STORE_ENTRY_EXISTS", INT2NUM(MDBM_STORE_ENTRY_EXISTS));
  rb_define_const(cMdbm, "MDBM_ENTRY_DELETED", INT2NUM(MDBM_ENTRY_DELETED));
  rb_define_const(cMdbm, "MDBM_ENTRY_LARGE_OBJECT", INT2NUM(MDBM_ENTRY_LARGE_OBJECT));
  rb_define_const(cMdbm, "MDBM_ITERATE_ENTRIES", INT2NUM(MDBM_ITERATE_ENTRIES));
  rb_define_const(cMdbm, "MDBM_ITERATE_NOLOCK", INT2NUM(MDBM_ITERATE_NOLOCK));
  rb_define_const(cMdbm, "MDBM_LOCKMODE_UNKNOWN", INT2NUM(MDBM_LOCKMODE_UNKNOWN));
  rb_define_const(cMdbm, "MDBM_CHECK_HEADER", INT2NUM(MDBM_CHECK_HEADER));
  rb_define_const(cMdbm, "MDBM_CHECK_CHUNKS", INT2NUM(MDBM_CHECK_CHUNKS));
  rb_define_const(cMdbm, "MDBM_CHECK_DIRECTORY", INT2NUM(MDBM_CHECK_DIRECTORY));
  rb_define_const(cMdbm, "MDBM_CHECK_ALL", INT2NUM(MDBM_CHECK_ALL));
  rb_define_const(cMdbm, "MDBM_PROT_NONE", INT2NUM(MDBM_PROT_NONE));
  rb_define_const(cMdbm, "MDBM_PROT_READ", INT2NUM(MDBM_PROT_READ));
  rb_define_const(cMdbm, "MDBM_PROT_WRITE", INT2NUM(MDBM_PROT_WRITE));
  rb_define_const(cMdbm, "MDBM_PROT_ACCESS", INT2NUM(MDBM_PROT_ACCESS));
  rb_define_const(cMdbm, "MDBM_CLOCK_STANDARD", INT2NUM(MDBM_CLOCK_STANDARD));
  rb_define_const(cMdbm, "MDBM_CLOCK_TSC", INT2NUM(MDBM_CLOCK_TSC));
  rb_define_const(cMdbm, "MDBM_STATS_BASIC", INT2NUM(MDBM_STATS_BASIC));
  rb_define_const(cMdbm, "MDBM_STATS_TIMED", INT2NUM(MDBM_STATS_TIMED));
  rb_define_const(cMdbm, "MDBM_STAT_CB_INC", INT2NUM(MDBM_STAT_CB_INC));
  rb_define_const(cMdbm, "MDBM_STAT_CB_SET", INT2NUM(MDBM_STAT_CB_SET));
  rb_define_const(cMdbm, "MDBM_STAT_CB_ELAPSED", INT2NUM(MDBM_STAT_CB_ELAPSED));
  rb_define_const(cMdbm, "MDBM_STAT_CB_TIME", INT2NUM(MDBM_STAT_CB_TIME));
  rb_define_const(cMdbm, "MDBM_STAT_TAG_FETCH", INT2NUM(MDBM_STAT_TAG_FETCH));
  rb_define_const(cMdbm, "MDBM_STAT_TAG_STORE", INT2NUM(MDBM_STAT_TAG_STORE));
  rb_define_const(cMdbm, "MDBM_STAT_TAG_DELETE", INT2NUM(MDBM_STAT_TAG_DELETE));
  rb_define_const(cMdbm, "MDBM_STAT_TAG_LOCK", INT2NUM(MDBM_STAT_TAG_LOCK));
  rb_define_const(cMdbm, "MDBM_STAT_TAG_FETCH_UNCACHED", INT2NUM(MDBM_STAT_TAG_FETCH_UNCACHED));
  rb_define_const(cMdbm, "MDBM_STAT_TAG_GETPAGE", INT2NUM(MDBM_STAT_TAG_GETPAGE));
  rb_define_const(cMdbm, "MDBM_STAT_TAG_GETPAGE_UNCACHED", INT2NUM(MDBM_STAT_TAG_GETPAGE_UNCACHED));
  rb_define_const(cMdbm, "MDBM_STAT_TAG_CACHE_EVICT", INT2NUM(MDBM_STAT_TAG_CACHE_EVICT));
  rb_define_const(cMdbm, "MDBM_STAT_TAG_CACHE_STORE", INT2NUM(MDBM_STAT_TAG_CACHE_STORE));
  rb_define_const(cMdbm, "MDBM_STAT_TAG_PAGE_STORE", INT2NUM(MDBM_STAT_TAG_PAGE_STORE));
  rb_define_const(cMdbm, "MDBM_STAT_TAG_PAGE_DELETE", INT2NUM(MDBM_STAT_TAG_PAGE_DELETE));
  rb_define_const(cMdbm, "MDBM_STAT_TAG_SYNC", INT2NUM(MDBM_STAT_TAG_SYNC));
  rb_define_const(cMdbm, "MDBM_STAT_TAG_FETCH_NOT_FOUND", INT2NUM(MDBM_STAT_TAG_FETCH_NOT_FOUND));
  rb_define_const(cMdbm, "MDBM_STAT_TAG_FETCH_ERROR", INT2NUM(MDBM_STAT_TAG_FETCH_ERROR));
  rb_define_const(cMdbm, "MDBM_STAT_TAG_STORE_ERROR", INT2NUM(MDBM_STAT_TAG_STORE_ERROR));
  rb_define_const(cMdbm, "MDBM_STAT_TAG_DELETE_FAILED", INT2NUM(MDBM_STAT_TAG_DELETE_FAILED));
  rb_define_const(cMdbm, "MDBM_STAT_DELETED", INT2NUM(MDBM_STAT_DELETED));
  rb_define_const(cMdbm, "MDBM_STAT_KEYS", INT2NUM(MDBM_STAT_KEYS));
  rb_define_const(cMdbm, "MDBM_STAT_VALUES", INT2NUM(MDBM_STAT_VALUES));
  rb_define_const(cMdbm, "MDBM_STAT_PAGES_ONLY", INT2NUM(MDBM_STAT_PAGES_ONLY));
  rb_define_const(cMdbm, "MDBM_STAT_NOLOCK", INT2NUM(MDBM_STAT_NOLOCK));
  rb_define_const(cMdbm, "MDBM_STAT_BUCKETS", INT2NUM(MDBM_STAT_BUCKETS));
  rb_define_const(cMdbm, "MDBM_CACHEMODE_NONE", INT2NUM(MDBM_CACHEMODE_NONE));
  rb_define_const(cMdbm, "MDBM_CACHEMODE_LFU", INT2NUM(MDBM_CACHEMODE_LFU));
  rb_define_const(cMdbm, "MDBM_CACHEMODE_LRU", INT2NUM(MDBM_CACHEMODE_LRU));
  rb_define_const(cMdbm, "MDBM_CACHEMODE_GDSF", INT2NUM(MDBM_CACHEMODE_GDSF));
  rb_define_const(cMdbm, "MDBM_CACHEMODE_MAX", INT2NUM(MDBM_CACHEMODE_MAX));
  rb_define_const(cMdbm, "MDBM_CACHEMODE_EVICT_CLEAN_FIRST", INT2NUM(MDBM_CACHEMODE_EVICT_CLEAN_FIRST));
  rb_define_const(cMdbm, "MDBM_MINPAGE", INT2NUM(MDBM_MINPAGE));
  rb_define_const(cMdbm, "MDBM_PAGE_ALIGN", INT2NUM(MDBM_PAGE_ALIGN));
  rb_define_const(cMdbm, "MDBM_PAGESIZ", INT2NUM(MDBM_PAGESIZ));
  rb_define_const(cMdbm, "MDBM_MIN_PSHIFT", INT2NUM(MDBM_MIN_PSHIFT));
  rb_define_const(cMdbm, "MDBM_HASH_CRC32", INT2NUM(MDBM_HASH_CRC32));
  rb_define_const(cMdbm, "MDBM_HASH_EJB", INT2NUM(MDBM_HASH_EJB));
  rb_define_const(cMdbm, "MDBM_HASH_PHONG", INT2NUM(MDBM_HASH_PHONG));
  rb_define_const(cMdbm, "MDBM_HASH_OZ", INT2NUM(MDBM_HASH_OZ));
  rb_define_const(cMdbm, "MDBM_HASH_TOREK", INT2NUM(MDBM_HASH_TOREK));
  rb_define_const(cMdbm, "MDBM_HASH_FNV", INT2NUM(MDBM_HASH_FNV));
  rb_define_const(cMdbm, "MDBM_HASH_STL", INT2NUM(MDBM_HASH_STL));
  rb_define_const(cMdbm, "MDBM_HASH_MD5", INT2NUM(MDBM_HASH_MD5));
  rb_define_const(cMdbm, "MDBM_HASH_SHA_1", INT2NUM(MDBM_HASH_SHA_1));
  rb_define_const(cMdbm, "MDBM_HASH_JENKINS", INT2NUM(MDBM_HASH_JENKINS));
  rb_define_const(cMdbm, "MDBM_HASH_HSIEH", INT2NUM(MDBM_HASH_HSIEH));
  rb_define_const(cMdbm, "MDBM_MAX_HASH", INT2NUM(MDBM_MAX_HASH));

}

