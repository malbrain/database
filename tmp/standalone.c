#define __STDC_WANT_LIB_EXT1__ 1
#define _DEFAULT_SOURCE 1

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "db.h"
#include "db_handle.h"
#include "db_api.h"

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <direct.h>
#include <process.h>
#include <windows.h>
#endif

#ifdef _WIN32
#define strncasecmp _strnicmp
#define fwrite_unlocked _fwrite_nolock
#define fputc_unlocked _fputc_nolock
#define getc_unlocked _getc_nolock
#else
#define fopen_s(file, path, mode) ((*file = fopen(path, mode)) ? 0 : errno)
#define sprintf_s snprintf
#endif

bool stats = true;

extern uint64_t totalMemoryReq[1];
extern uint64_t nodeAlloc[64];
extern uint64_t nodeFree[64];
extern uint64_t nodeWait[64];

bool debug = false;
bool dropDb = false;
bool noExit = false;
bool noDocs = false;
bool noIdx = false;
bool monitor = false;
bool pipeLine = false;
bool pennysort = false;

char *idxName;
int numThreads = 1;
char binaryFlds = 0;

double getCpuTime(int type);

void printBinary(uint8_t fieldDelim, uint8_t *key, int len, FILE *file) {
  int off = 0;

  while (off < len) {
    int fldLen = key[off] << 8 | key[off + 1];
    if (off) fputc_unlocked(fieldDelim, file);
    off += 2;
    fwrite_unlocked(key + off, fldLen, 1, file);
    off += fldLen;
  }
}

int prng = 0, offset = 0;

typedef struct {
  char *cmds;
  char *inFile;
  Params *params;
  int keyLen, idx;
  DbHandle dbHndl[1];
  DbHandle iterator[1];
  DbHandle docHndl[1];
  DbHandle cursor[1];
  DbHandle idxHndl[1];
  bool noExit;
  bool noDocs;
  bool noIdx;
  uint64_t line;
  int offset;
} ThreadArgs;

typedef struct {
  char *cmds;
  char *minKey;
  char *maxKey;
  int offset;
  Params *params;
  DbHandle *dbHndl;
  DbHandle *iterator;
  DbHandle *docHndl;
  DbHandle *cursor;
  DbHandle *idxHndl;
  bool noExit;
  bool noDocs;
  bool noIdx;
} ScanArgs;

char *indexNames[] = {"ARTree", "Btree1", "Btree2"};

HandleType indexType[] = {Hndl_artIndex, Hndl_btree1Index, Hndl_btree2Index};

//	our documents in the docStore

typedef struct {
  uint32_t size;
} Doc;

void myExit(ThreadArgs *args) {
  if (args->noExit) return;

  exit(1);
}

//  standalone program to index file of keys

int index_file(ThreadArgs *args, char cmd, char *msg, int msgMax) {
  uint16_t nrandState[3];
  int msgLen = 0;
  uint8_t rec[4096];
  Doc *doc = (Doc *)rec;
  uint8_t *body = rec + sizeof(Doc);
  uint8_t keyBuff[4096];
  KeyValue *kv = (KeyValue *)keyBuff;
  int keyLen = 0, docLen = 0;
  int ch, keyOff, docMax, keyMax;
  uint32_t foundLen = 0;
  int lastFld = 0;
  uint8_t *foundKey;
  uint64_t count = ~0ULL;
  ObjId docId;
  FILE *in;
  DbStatus stat;
  uint8_t *key = kv->bytes;
  docMax = 4096 - sizeof(Doc);

  if (pennysort) docMax = 100;

  mynrand48seed(nrandState, prng, args->idx + args->offset);
  msg[msgLen++] = '\n';

  switch (cmd | 0x20) {
    case 'd':
      msgLen += sprintf_s(msg + msgLen, msgMax - msgLen - 1,
                          "thrd:%d cmd:%c %s: begin delete keys file:%s\n",
                          args->idx, cmd, idxName, args->inFile);
      break;
    case 'f':
      msgLen += sprintf_s(msg + msgLen, msgMax - msgLen - 1,
                          "thrd:%d cmd:%c %s: begin find keys file:%s\n",
                          args->idx, cmd, idxName, args->inFile);
      break;
    case 'w':
      msgLen += sprintf_s(msg + msgLen, msgMax - msgLen - 1,
                          "thrd:%d cmd:%c %s: begin add keys file:%s\n",
                          args->idx, cmd, idxName, args->inFile);
      break;
  }

  if (pennysort) {
    msgLen += sprintf_s(msg + msgLen, msgMax - msgLen - 1,
                        "thrd:%d cmd:%c %s: prng:%d 10 byte pennysort keys\n",
                        args->idx, cmd, idxName, prng);
    count = atoi(args->inFile);
  } else if (fopen_s(&in, args->inFile, "r"))
    return msgLen + sprintf_s(msg + msgLen, msgMax - msgLen - 1,
                              "thrd:%d cmd:%c file:%s unable to open\n",
                              args->idx, cmd, args->inFile);

  //  read or generate next doc and key

  while (args->line < count) {
    docLen = 0;

    keyOff = binaryFlds ? 2 : 0;
    keyLen = keyOff;
    lastFld = 0;

    if (pennysort) {
      keyMax = args->keyLen ? args->keyLen : 10;
      docLen = createB64(body, keyMax, nrandState);

      while (docLen < 100) {
        body[docLen++] = '\t';
        memset(body + docLen, 'A' + docLen / 10, 9);
        docLen += 9;
      }

      if (args->line == 0) {
        msgLen += sprintf_s(msg + msgLen, msgMax - msgLen - 1,
                            "thrd:%d cmd:%c %s: first key: <%.10s>", args->idx,
                            cmd, idxName, body);
        if (args->offset)
          msgLen += sprintf_s(msg + msgLen, msgMax - msgLen - 1,
                              "lineno offset: %u", args->offset);
        msg[msgLen++] = '\n';
      }

      memcpy(key + keyOff, body, keyMax);
      keyLen += keyMax;
    } else {
      keyMax = 4096;

      while (ch = getc_unlocked(in), ch != EOF && ch != '\n') {
        if (!args->noDocs)
          if (docLen < docMax) body[docLen++] = (uint8_t)ch;

        if (!args->noIdx)
          if (docLen < keyMax) {
            if (binaryFlds)
              if (ch == '\t' || ch == binaryFlds) {
                key[lastFld] = (uint8_t)((keyLen - lastFld - 2) >> 8);
                key[lastFld + 1] = (uint8_t)(keyLen - lastFld - 2);
                lastFld = keyLen;
                if (ch == '\t')
                  keyMax = 0;
                else
                  keyLen += 2;
              } else
                key[keyLen++] = (uint8_t)ch;
          }
      }

      if (ch == EOF) break;
    }

    // add or delete next record to collection and index

    if (!(++args->line % 1000000) && monitor)
      fprintf(stderr, "thrd:%d cmd:%c line: %" PRIu64 "\n", args->idx, cmd,
              args->line);

    if (binaryFlds && keyMax) {
      key[lastFld] = (uint8_t)((keyLen - lastFld - 2) >> 8);
      key[lastFld + 1] = (uint8_t)(keyLen - lastFld - 2);
    }

    lastFld = 0;

    switch (cmd | 0x20) {
      case 'w':

        // store the entry in the docStore?

        if (args->docHndl->hndlId.bits) {
          doc->size = docLen;
          if ((stat =
                   storeDoc(args->docHndl, doc, sizeof(Doc) + docLen, &docId)))
            fprintf(stderr, "Add Doc %s Error %d Line: %" PRIu64 " *********\n",
                    args->inFile, stat, args->line),
                exit(0);
        } else
          docId.bits = args->line + args->offset;

        if (args->idxHndl->hndlId.bits) {

          kv->suffixLen = store64(key, keyLen, docId.bits);
          kv->keyLen = keyLen;

          switch ((stat = insertKeyValue(args->idxHndl, kv))) {
            case DB_ERROR_unique_key_constraint:
              fprintf(stderr, "Duplicate key <%.*s> line: %" PRIu64 "\n",
                      keyLen, key, args->line);
              break;
            case DB_OK:
              break;
            default:
              return msgLen += sprintf_s(msg + msgLen, msgMax - msgLen - 1,
                                        "thrd:%d cmd:%c file:%s InsertKey "
                                        "dbError:%d errno:%d line: %" PRIu64
                                        " **********\n",
                                        args->idx, cmd, args->inFile, stat,
                                        errno, args->line);
          }
        }

        break;

        //	delete key

      case 'd':

        if ((stat = deleteKey(args->idxHndl, key, keyLen)))
          return msgLen + sprintf_s(msg + msgLen, msgMax - msgLen - 1,
                                    "thrd:%d cmd:%c file:%s deleteKey "
                                    "dbError:%d errno:%d line: %" PRIu64
                                    " **********\n",
                                    args->idx, cmd, args->inFile, stat, errno,
                                    args->line);

        break;

        //	find record by key

      case 'f':
        if ((stat = positionCursor(args->cursor, OpOne, key, keyLen)))
          return msgLen + sprintf_s(msg + msgLen, msgMax - msgLen - 1,
                                    "thrd:%d cmd:%c file:%s findKey dbError:%d "
                                    "errno: %d line: %" PRIu64 " **********\n",
                                    args->idx, cmd, args->inFile, stat, errno,
                                    args->line);

        if ((stat = keyAtCursor(args->cursor, &foundKey, &foundLen)))
          return msgLen + sprintf_s(msg + msgLen, msgMax - msgLen - 1,
                                    "thrd:%d cmd:%c file:%s findKey dbError:%d "
                                    "errno: %d line: %" PRIu64 " **********\n",
                                    args->idx, cmd, args->inFile, stat, errno,
                                    args->line);

        if (args->docHndl->hndlId.bits && args->idxHndl->hndlId.bits) {
          docId.bits = get64(foundKey, foundLen);
          foundLen -= size64(foundKey, foundLen);
        }

        if (!binaryFlds)
          if (memcmp(foundKey, key, keyLen))
            fprintf(stderr,
                    "findKey %s not Found: line: %" PRIu64 " expected: %.*s \n",
                    args->inFile, args->line, keyLen, key),
                myExit(args);

        break;
    }
  }

  msgLen += sprintf_s(msg + msgLen, msgMax - msgLen - 1,
                      "thrd:%d cmd:%c %s: end: %" PRIu64 " records processed\n",
                      args->idx, cmd, idxName, args->line);

  msg[msgLen++] = '\n';

  closeHandle(args->docHndl);
  closeHandle(args->idxHndl);
  closeHandle(args->cursor);
  closeHandle(args->dbHndl);

  return msgLen;
}

uint64_t index_scan(ScanArgs *scan, DbHandle *database) {
  uint64_t cnt = 0;
  uint8_t rec[4096];
  Doc *doc = (Doc *)rec;

  char cmd;
  uint32_t foundLen = 0;
  uint32_t prevLen = 0;
  double startx1 = getCpuTime(0);
  double startx2 = getCpuTime(1);
  double startx3 = getCpuTime(2);
  double elapsed;
  bool numbers = false;
  bool keyList = false;
  bool reverse = false;
  bool cntOnly = false;
  bool verify = false;
  bool dump = false;
  uint8_t *foundKey;
  ObjId docId;
  int stat;

  fprintf(stderr, "\nIndex %s summary scan:\n", idxName);

  while ((cmd = *scan->cmds++)) switch (cmd | 0x20) {
      case 'i':
        fprintf(stderr, " iterator scan\n");

        if (scan->noDocs)
          fprintf(stderr, "Cannot specify noDocs with iterator scan\n"),
              exit(0);

        while ((moveIterator(scan->iterator, IterNext, (void **)&doc, &docId) ==
                DB_OK)) {
          if (dump) {
            fwrite_unlocked(doc + 1, doc->size, 1, stdout);
            fputc_unlocked('\n', stdout);
          }
          cnt++;
        }

        fprintf(stderr, " iterator scan total: %" PRIu64 "\n", cnt);
        return cnt;

      case 'd':
        dump = true;
        continue;

      case 'n':
        numbers = true;
        continue;

      case 'k':
        keyList = true;
        continue;

      case 'v':
        verify = true;
        continue;

      case 'c':
        cntOnly = true;
        continue;

      case 'r':
        reverse = true;
        continue;

      case 's':
        break;
    }

  if (reverse)
    fprintf(stderr, " reverse index cursor\n");

  else
    fprintf(stderr, " forward index cursor\n");

  if (verify) fprintf(stderr, " key order verification\n");

  if (cntOnly) fprintf(stderr, " index key count\n");

  if (dump) fprintf(stderr, " doc/key dump\n");

  if (scan->minKey) fprintf(stderr, " min key: <%s>\n", scan->minKey);

  if (scan->maxKey) fprintf(stderr, " max key: <%s>\n", scan->maxKey);

  if (!reverse && scan->minKey)
    stat = positionCursor(scan->cursor, OpBefore, scan->minKey,
                          (int)strlen(scan->minKey));
  else if (reverse && scan->maxKey)
    stat = positionCursor(scan->cursor, OpAfter, scan->maxKey,
                          (int)strlen(scan->maxKey));
  else
    stat = moveCursor(scan->cursor, reverse ? OpRight : OpLeft);

  if (stat) fprintf(stderr, "positionCursor Error %d\n", stat), exit(0);

  while (!(stat = moveCursor(scan->cursor, reverse ? OpPrev : OpNext))) {
    uint32_t keyLen;

    if ((stat = keyAtCursor(scan->cursor, &foundKey, &foundLen)))
      fprintf(stderr, "keyAtCursor Error %d\n", stat), exit(0);

    keyLen = foundLen - size64(foundKey, foundLen);

    if (reverse && scan->minKey)
      if (memcmp(foundKey, scan->minKey, (int)strlen(scan->minKey)) < 0) break;
    if (!reverse && scan->maxKey)
      if (memcmp(foundKey, scan->maxKey, (int)strlen(scan->maxKey)) > 0) break;

    cnt++;

    if (verify) {
      if (prevLen == keyLen) {
        int comp = memcmp(foundKey, rec, prevLen);

        if ((reverse && comp > 0) || (!reverse && comp < 0))
          fprintf(stderr, "verify: Key compare error: %d\n", (int)cnt), exit(0);
      }

      prevLen = keyLen;
      memcpy(rec, foundKey, keyLen);
    }

    if (cntOnly) {
      if (monitor && !(cnt % 1000000))
        fprintf(stderr, " scan %s count: %" PRIu64 "\n", idxName, cnt);
      continue;
    }

    if (numbers && scan->noDocs) {
      uint64_t lineNo = get64(foundKey, foundLen);
      fprintf(stdout, "%.12" PRIu64 "\t", lineNo);
    }

    if (keyList)
      if (binaryFlds)
        printBinary(binaryFlds, foundKey, keyLen, stdout);
      else
        fwrite_unlocked(foundKey, keyLen, 1, stdout);

    if (numbers || keyList) fputc_unlocked('\t', stdout);

    if (dump && !scan->noDocs) {
      docId.bits = get64(foundKey, foundLen);
      fetchDoc(scan->docHndl, (void **)&doc, docId);
      fwrite_unlocked(doc + 1, doc->size, 1, stdout);
    }

    if (numbers || keyList || dump) fputc_unlocked('\n', stdout);
  }

  if (stat && stat != DB_CURSOR_eof)
    fprintf(stderr, " Index %s Scan: Error %d Syserr %d Line: %" PRIu64 "\n",
            idxName, stat, errno, cnt),
        exit(0);

  fprintf(stderr, " Index scan complete\n Total keys %" PRIu64 "\n\n", cnt);

  elapsed = getCpuTime(0) - startx1;
  fprintf(stderr, " real %dm%.3fs\n", (int)(elapsed / 60),
          elapsed - (int)(elapsed / 60) * 60);

  elapsed = getCpuTime(1) - startx2;
  fprintf(stderr, " user %dm%.3fs\n", (int)(elapsed / 60),
          elapsed - (int)(elapsed / 60) * 60);

  elapsed = getCpuTime(2) - startx3;
  fprintf(stderr, " sys  %dm%.3fs\n", (int)(elapsed / 60),
          elapsed - (int)(elapsed / 60) * 60);

  return cnt;
}

//  run index_file once for each command

#ifndef _WIN32
void *pipego(void *arg) {
#else
unsigned __stdcall pipego(void *arg) {
#endif
  ThreadArgs *args = arg;
  int len = 0, max;
  char cmd;
  char msg[4096];
  double elapsed;

  while ((cmd = *args->cmds++) && len < sizeof(msg) / 3) {
    double startx1 = getCpuTime(0);
    double startx2 = getCpuTime(1);
    double startx3 = getCpuTime(2);

    max = sizeof(msg) - len - 1;
    len += index_file(args, cmd, msg + len, max);

    elapsed = getCpuTime(0) - startx1;

    len += sprintf_s(msg + len, sizeof(msg) - len - 1, " real %dm%.3fs\n",
                     (int)(elapsed / 60), elapsed - (int)(elapsed / 60) * 60);

    elapsed = getCpuTime(1) - startx2;

    len += sprintf_s(msg + len, sizeof(msg) - len - 1, " user %dm%.3fs\n",
                     (int)(elapsed / 60), elapsed - (int)(elapsed / 60) * 60);

    elapsed = getCpuTime(2) - startx3;

    len += sprintf_s(msg + len, sizeof(msg) - len - 1, " sys  %dm%.3fs\n",
                     (int)(elapsed / 60), elapsed - (int)(elapsed / 60) * 60);
  }

  fwrite(msg, 1ULL, len, stderr);

#ifndef _WIN32
  return NULL;
#else
  return 0;
#endif
}

int main(int argc, char **argv) {
  Params params[MaxParam];
  char *summary = NULL;
  char *dbName = NULL;
  char *cmds = NULL;
  int keyLen = 10;
  int idx, cnt;

#ifndef _WIN32
  pthread_t *threads;
#else
  SYSTEM_INFO info[1];
  HANDLE *threads;
#endif

  DbHandle iterator[1];
  DbHandle docHndl[1];
  DbHandle cursor[1];
  DbHandle idxHndl[1];
  DbHandle dbHndl[1];
  uint64_t totKeys = 0;
  ThreadArgs *args;
  DbStatus stat;
  DbHandle *parent;
  ScanArgs scan[1];

  memset(scan, 0, sizeof(ScanArgs));
  iterator->hndlId.bits = 0;
  docHndl->hndlId.bits = 0;
  cursor->hndlId.bits = 0;
  idxHndl->hndlId.bits = 0;
  dbHndl->hndlId.bits = 0;

  if (argc < 3) {
    fprintf(stderr,
            "Usage: %s db_name -cmds=[wdf] -summary=[csrvikdn] -idxType=[012] "
            "-bits=# -xtra=# -inMem -stats -prng=# -debug -monitor -uniqueKeys "
            "-noDocs -noIdx -keyLen=# -minKey=abcd -maxKey=abce -drop "
            "-idxBinary=. -pipeline -offset=# src_file1 src_file2 ... ]\n",
            argv[0]);
    fprintf(stderr,
            "  where db_name is the prefix name of the database document and "
            "index files\n");
    fprintf(stderr,
            "  cmds is a string of (w)rite/(d)elete/(f)ind commands, to run "
            "sequentially on each input src_file.\n");
    fprintf(stderr,
            "  summary scan is a string of (c)ount/(r)everse "
            "scan/(s)can/(v)erify/(i)terate/(k)ey list(d)ump doc(n)umber flags "
            "for a final scan after all threads have quit\n");
    fprintf(stderr,
            "  pennysort creates random 100 byte B64 input lines, sets keyLen "
            "to 10, line count from the file name\n");
    fprintf(
        stderr,
        "  idxType is the type of index: 0 = ART, 1 = btree1, 2 = btree2\n");
    fprintf(stderr, "  keyLen is key size, zero for whole input file line\n");
    fprintf(stderr, "  bits is the btree page size in bits\n");
    fprintf(stderr, "  xtra is the btree leaf page extra bits\n");
    fprintf(stderr, "  inMem specifies no disk files\n");
    fprintf(stderr, "  noDocs specifies keys only\n");
    fprintf(stderr, "  noIdx specifies documents only\n");
    fprintf(stderr, "  minKey specifies beginning cursor key\n");
    fprintf(stderr, "  maxKey specifies ending cursor key\n");
    fprintf(stderr, "  drop will initially drop database\n");
    fprintf(stderr,
            "  idxBinary utilize length counted fields separated by  the given "
            "deliminator in keys\n");
    fprintf(stderr, "  use prng number stream [012] for pennysort keys\n");
    fprintf(stderr,
            "  stats keeps performance and debugging totals and prints them\n");
    fprintf(stderr, "  uniqueKeys ensure keys are unique\n");
    fprintf(stderr,
            "  offset of first line number in key suffix document ID\n");
    fprintf(stderr,
            "  run cmds in a single threaded  pipeline using one input file at "
            "a time\n");
    fprintf(stderr,
            "  src_file1 thru src_filen are files of keys/documents separated "
            "by newline\n");
    exit(0);
  }

  // process database name

  dbName = (++argv)[0];
  argc--;

  //	set default values

  memset(params, 0, sizeof(params));
  params[Btree1Bits].intVal = 14;
  params[OnDisk].boolVal = true;
  params[Btree2Bits].intVal = 14;

  // process configuration arguments

  while (--argc > 0 && (++argv)[0][0] == '-')
    if (!strncasecmp(argv[0], "-xtra=", 6)) {
      params[Btree1Xtra].intVal = atoi(argv[0] + 6);
      params[Btree2Xtra].intVal = atoi(argv[0] + 6);
    } else if (!strncasecmp(argv[0], "-keyLen=", 8))
      keyLen = atoi(argv[0] + 8);
    else if (!strncasecmp(argv[0], "-bits=", 6)) {
      params[Btree1Bits].intVal = atoi(argv[0] + 6);
      params[Btree2Bits].intVal = atoi(argv[0] + 6);
    } else if (!strncasecmp(argv[0], "-cmds=", 6))
      cmds = argv[0] + 6;
    else if (!strncasecmp(argv[0], "-debug", 6))
      debug = true;
    else if (!strncasecmp(argv[0], "-monitor", 8))
      monitor = true;
    else if (!strncasecmp(argv[0], "-stats", 6))
      stats = true;
    else if (!strncasecmp(argv[0], "-prng=", 6))
      prng = atoi(argv[0] + 6);
    else if (!strncasecmp(argv[0], "-offset=", 8))
      offset = atoi(argv[0] + 8);
    else if (!strncasecmp(argv[0], "-drop", 5))
      dropDb = true;
    else if (!strncasecmp(argv[0], "-noIdx", 6))
      noIdx = true;
    else if (!strncasecmp(argv[0], "-noDocs", 7))
      noDocs = true;
    else if (!strncasecmp(argv[0], "-pennysort", 10))
      pennysort = true;
    else if (!strncasecmp(argv[0], "-pipeline", 9))
      pipeLine = true;
    else if (!strncasecmp(argv[0], "-noExit", 7))
      noExit = true;
    else if (!strncasecmp(argv[0], "-uniqueKeys", 11))
      params[IdxKeyUnique].boolVal = true;
    else if (!strncasecmp(argv[0], "-idxType=", 9))
      params[IdxType].intVal = atoi(argv[0] + 9);
    else if (!strncasecmp(argv[0], "-inMem", 6))
      params[OnDisk].boolVal = false;
    else if (!strncasecmp(argv[0], "-idxBinary=", 11))
      params[IdxKeyFlds].charVal = argv[0][11];
    else if (!strncasecmp(argv[0], "-minKey=", 8))
      scan->minKey = argv[0] + 8;
    else if (!strncasecmp(argv[0], "-maxKey=", 8))
      scan->maxKey = argv[0] + 8;
    else if (!strncasecmp(argv[0], "-threads=", 9))
      numThreads = atoi(argv[0] + 9);
    else if (!strncasecmp(argv[0], "-summary=", 9))
      summary = argv[0] + 9;
    else
      fprintf(stderr, "Unknown option %s ignored\n", argv[0]);

  cnt = numThreads > argc ? numThreads : argc;
  idxName = indexNames[params[IdxType].intVal];
  binaryFlds = params[IdxKeyFlds].charVal;

  initialize();

#ifndef _WIN32
  threads = malloc(cnt * sizeof(pthread_t));
#else
  threads = GlobalAlloc(GMEM_FIXED | GMEM_ZEROINIT, cnt * sizeof(HANDLE));
#endif
  args = malloc((cnt ? cnt : 1) * sizeof(ThreadArgs));

  openDatabase(dbHndl, dbName, (int)strlen(dbName), params);

  //  drop the database?

  if (dropDb) {
    dropArena(dbHndl, true);
    openDatabase(dbHndl, dbName, (int)strlen(dbName), params);
  }

  parent = dbHndl;

  if (!noDocs) {
    if ((stat = openDocStore(docHndl, dbHndl, "documents",
                             (int)strlen("documents"), params)))
      fprintf(stderr, "file:%s unable to open error %d\n", "docStore", stat),
          exit(3);

    parent = docHndl;

    if ((stat = createIterator(iterator, docHndl, scan->params)))
      fprintf(stderr, "createIterator Error %d\n", stat), exit(0);
  }

  if (!noIdx) {
    if ((stat = createIndex(idxHndl, parent, idxName, (int)strlen(idxName),
                            params)))
      fprintf(stderr, "file:%s unable to open error %d\n", idxName, stat),
          exit(3);

    createCursor(cursor, idxHndl, params);
  }

  if (noDocs && noIdx)
    fprintf(stderr, "Cannot specify both -noDocs and -noIdx\n"), exit(3);

  //	fire off threads

  idx = 0;

  do {
#ifndef _WIN32
    int err;
#endif
    memset(args + idx, 0, sizeof(*args));

    args[idx].inFile = idx < argc ? argv[idx] : argv[argc - 1];
    args[idx].dbHndl->hndlId.bits = dbHndl->hndlId.bits;

    cloneHandle(args[idx].docHndl, docHndl);
    cloneHandle(args[idx].iterator, iterator);
    cloneHandle(args[idx].docHndl, docHndl);
    cloneHandle(args[idx].cursor, cursor);
    cloneHandle(args[idx].idxHndl, idxHndl);

    args[idx].params = params;
    args[idx].keyLen = keyLen;
    args[idx].noDocs = noDocs;
    args[idx].offset = offset;
    args[idx].noExit = noExit;
    args[idx].noIdx = noIdx;
    args[idx].cmds = cmds;
    args[idx].idx = idx;

    if (pipeLine || cnt == 1) {
      pipego(args);
      continue;
    }

#ifndef _WIN32
    if ((err = pthread_create(threads + idx, NULL, pipego, args + idx)))
      fprintf(stderr, "Error creating thread %d\n", err);
#else
    while (((int64_t)(threads[idx] = (HANDLE)_beginthreadex(
                          NULL, 65536, pipego, args + idx, 0, NULL)) < 0LL))
      fprintf(stderr, "Error creating thread errno = %d\n", errno);
#endif

    fprintf(stderr, "thread %d launched for file %s cmds %s\n", idx,
            args[idx].inFile, cmds);
  } while (++idx < cnt);

  // 	wait for termination

  if (!pipeLine && cnt > 1) {
#ifndef _WIN32
    for (idx = 0; idx < cnt; idx++) pthread_join(threads[idx], NULL);
#else
    WaitForMultipleObjects(cnt, threads, TRUE, INFINITE);

    for (idx = 0; idx < cnt; idx++) CloseHandle(threads[idx]);
#endif
  }

  scan->dbHndl = dbHndl;
  scan->iterator = iterator;
  scan->docHndl = docHndl;
  scan->cursor = cursor;
  scan->idxHndl = idxHndl;
  scan->params = params;
  scan->noDocs = noDocs;
  scan->noExit = noExit;
  scan->noIdx = noIdx;
  scan->offset = offset;

  if ((scan->cmds = summary)) totKeys = index_scan(scan, dbHndl);

  if (stats) {
    fputc(0x0a, stderr);

    if (totKeys) {
      fprintf(stderr, "Total memory allocated: %.3f MB\n",
              (double)*totalMemoryReq / (1024. * 1024.));
      fprintf(stderr, "Bytes per key: %" PRIu64 "\n",
              *totalMemoryReq / totKeys);
    }

    switch (params[IdxType].intVal) {
      case 0:
        fputc(0x0a, stderr);

        for (idx = 0; idx < 64; idx++)
          if (nodeAlloc[idx])
            fprintf(stderr, "%s Index type %d blks allocated: %" PRIu64 "\n",
                    idxName, idx, nodeAlloc[idx]);

        fputc(0x0a, stderr);

        for (idx = 0; idx < 64; idx++)
          if (nodeFree[idx])
            fprintf(stderr, "%s Index type %d blks freed    : %" PRIu64 "\n",
                    idxName, idx, nodeFree[idx]);

        fputc(0x0a, stderr);

        for (idx = 0; idx < 64; idx++)
          if (nodeWait[idx])
            fprintf(stderr, "%s Index type %d blks recycled : %" PRIu64 "\n",
                    idxName, idx, nodeWait[idx]);

        fputc(0x0a, stderr);
        break;
    }
  }

  closeHandle(docHndl);
  closeHandle(idxHndl);
  closeHandle(cursor);
  closeHandle(dbHndl);
  closeHandle(iterator);

  if (debug) {
#ifdef _WIN32
    char buf[512];
    GetSystemInfo(info);
    fprintf(stderr,
            "\nCWD: %s PageSize: %d, # Processors: %d, Allocation Granularity: "
            "%d\n\n",
            _getcwd(buf, 512), (int)info->dwPageSize,
            (int)info->dwNumberOfProcessors,
            (int)info->dwAllocationGranularity);
#endif
  }
}
