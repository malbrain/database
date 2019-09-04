#define __STDC_WANT_LIB_EXT1__ 1
#define _DEFAULT_SOURCE 1

#include <errno.h>
#include <string.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>

#include "db.h"
#include "db_api.h"

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <direct.h>
#include <process.h>
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
bool pipeLine = false;
bool pennysort = false;
bool numbers = false;
bool keyList = false;

char* idxName;
int numThreads = 1;
char binaryFlds = 0;

double getCpuTime(int type);

void mynrand48seed(uint16_t *nrandState);
int createB64(char *key, int size, unsigned short next[3]);
uint64_t get64(uint8_t *key, uint32_t len);

void printBinary(char binaryFlds, uint8_t *key, int len, FILE *file) {
int off = 0;

	while(off < len) {
		int fldLen = key[off] << 8 | key[off + 1];
		if (off)
			fputc_unlocked (binaryFlds, file);
		off += 2;
		fwrite_unlocked (key + off, fldLen, 1, file);
		off += fldLen;
	}
}

typedef struct {
	char *cmds;
	char *scanCmd;
	uint8_t idx;
	char *inFile;
	char *minKey;
	char *maxKey;
	Params *params;
	DbHandle *database;
	int keyLen;
	bool noExit;
	bool noDocs;
	bool noIdx;
} ThreadArg;

char *indexNames[] = {
	"Adaptive Radix Tree",
	"Btree1 paged arrays of keys",
	"Btree2 paged skiplists"
};

HandleType indexType[] = {
Hndl_artIndex,
Hndl_btree1Index,
Hndl_btree2Index
};

//	our documents in the docStore

typedef struct {
	uint32_t size;
} Doc;

void myExit (ThreadArg *args) {
	if (args->noExit)
		return;

	exit(1);
}

//  standalone program to index file of keys

int index_file(ThreadArg *args, char cmd, char *msg, int msgMax)
{
uint64_t line = 0, cnt = 0;
DbHandle database[1];
DbHandle iterator[1];
DbHandle docHndl[1];
DbHandle cursor[1];
DbHandle idxHndl[1];
DbHandle *parent;
uint16_t nrandState[3];
int msgLen = 0;
uint8_t rec[4096];
Doc *doc = (Doc *)rec;
uint8_t *body = rec + sizeof(Doc);
uint8_t key[4096];
int keyLen = 0, docLen = 0;
int ch, len = 0, keyOff;
int docMax, keyMax;
uint32_t foundLen = 0;
uint32_t prevLen = 0;
bool reverse = false;
bool cntOnly = false;
bool verify = false;
int lastFld = 0;
uint8_t *foundKey;
uint64_t count = ~0ULL;
ObjId docId;
FILE *in;
DbStatus stat;

    docMax = 4096 - sizeof(Doc);

	if( pennysort )
		docMax = 100;

	if (debug)
		memset(nrandState, 0, sizeof(nrandState));
	else
		mynrand48seed(nrandState);

	nrandState[0] ^= args->idx;

	cloneHandle(database, args->database);

	iterator->hndlBits = 0;
	docHndl->hndlBits = 0;
	cursor->hndlBits = 0;
	idxHndl->hndlBits = 0;

	docHndl->hndlBits = 0;

	switch(cmd | 0x20) 	{

	case 'd':
		msgLen += sprintf_s(msg + msgLen, msgMax - msgLen - 1, "thrd:%d cmd:%c file:%s delete keys\n", args->idx, cmd, args->inFile);

		parent = database;

		if ((stat = createIndex(idxHndl, parent, idxName, (int)strlen(idxName), args->params)))
		  fprintf(stderr, "createIndex %s Error %d type: %s\n", args->inFile, stat, idxName), exit(0);

		createCursor (cursor, idxHndl, args->params);

		if (binaryFlds)
			len = 2;

		if( !fopen_s (&in, args->inFile, "r") )
		  while( ch = getc_unlocked(in), ch != EOF ) {
			if( ch == '\n' ) {
			  if (binaryFlds) {
				key[lastFld] = (len - lastFld - 2) >> 8;
				key[lastFld + 1] = (len - lastFld - 2);
			  }

			  line++;

			  if (debug && !(line % 100000))
				fprintf(stderr, "line %" PRIu64 "\n", line);

			  if ((stat = deleteKey(idxHndl, key, len)))
				  fprintf(stderr, "Delete Key %s Error %d Line: %" PRIu64 "\n", args->inFile, stat, line), myExit(args);

			  len = binaryFlds ? 2 : 0;
			  lastFld = 0;
			  continue;
			}

			if( len < 4096 ) {
			  if (binaryFlds) {
				if (ch == binaryFlds) {
					key[lastFld] = (len - lastFld - 2) >> 8;
					key[lastFld + 1] = (len - lastFld - 2);
					lastFld = len;
					len += 2;
					continue;
				}
			  }
			  key[len++] = ch;
			}
		}

		msgLen += sprintf_s(msg + msgLen, msgMax - msgLen - 1, "thrd:%d cmd:%c file:%s keys processed %" PRIu64 " \n", args->idx, cmd, args->inFile, line);

		break;

	case 'w':
		if( pennysort ) {
		  msgLen += sprintf_s(msg + msgLen, msgMax - msgLen - 1, "thrd:%d cmd:%c random keys:%s\n", args->idx, cmd, args->inFile);
		  count = atoi(args->inFile);
		} else {
		  msgLen += sprintf_s(msg + msgLen, msgMax - msgLen - 1, "thrd:%d cmd:%c file:%s load records\n", args->idx, cmd, args->inFile);
		  if(fopen_s (&in, args->inFile, "r")) {
		    msgLen += sprintf_s(msg + msgLen, msgMax - msgLen - 1, "thrd:%d cmd:%c file:%s unable to open\n", args->idx, cmd, args->inFile);
			break;
		  }
		}

		parent = database;

		if (!args->noDocs) {
		  if ((stat = openDocStore(docHndl, database, "documents", (int)strlen("documents"), args->params)))
			fprintf(stderr, "openDocStore %s Error %d name: %s\n", args->inFile, stat, "documents"), exit(0);
		  parent = docHndl;
		}

		if (!args->noIdx)
		  if ((stat = createIndex(idxHndl, parent, idxName, (int)strlen(idxName), args->params)))
			fprintf(stderr, "createIndex %s Error %d type: %s\n", args->inFile, stat, idxName), exit(0);

		if (args->noDocs && args->noIdx)
		  fprintf(stderr, "Cannot specify both -noDocs and -noIdx\n"), exit(0);

		//  read or generate next doc and key
		
		ch = 0;

		while( line < count ) {
		  docLen = 0;

		  keyOff = binaryFlds ? 2 : 0;
		  keyLen = keyOff;
		  lastFld = 0;

		  if( pennysort ) {
		    keyMax = args->keyLen ? args->keyLen : 10;
			docLen = createB64 (body, keyMax, nrandState);
		    while( docLen < 100 ) {
			   body[docLen++] = '\t';
			   memset(body + docLen, 'A' + docLen / 10, 9);
			   docLen += 9;
			 }
			 memcpy(key + keyOff, body, keyMax);
			 keyLen += keyMax;
		   } else {
		    keyMax = 4096;

		    while( ch = getc_unlocked(in), ch != EOF && ch != '\n' ) {
			 if (!args->noDocs)
			  if( docLen < 4096 - sizeof(Doc))
				  body[docLen++] = ch;

			 if(!args->noIdx)
			  if( docLen < keyMax ) {
			   if (binaryFlds)
				if( ch == '\t' || ch == binaryFlds) {
					key[lastFld] = (keyLen - lastFld - 2) >> 8;
					key[lastFld + 1] = (keyLen - lastFld - 2);
					lastFld = keyLen;
				    if( ch == '\t' )
					  keyMax = 0;
					else
					  keyLen += 2;
/*					  newFld = true;
				} else if(newFld) {
				    keyLen += 2;
					newFld = false;
*/				} else
					key[keyLen++] = ch;
			  }
			}

			if( ch == EOF )
			  break;
		  }

		  // add next record to collection and index

          if (!(++line % 1000000) && debug)
		    fprintf(stderr, "thrd:%d cmd:%c line: %" PRIu64 "\n", args->idx, cmd, line);

		  if (binaryFlds && keyMax) {
			key[lastFld] = (keyLen - lastFld - 2) >> 8;
			key[lastFld + 1] = (keyLen - lastFld - 2);
		  }

		  lastFld = 0;

		  // store the entry in the docStore?

		  if (docHndl->hndlBits) {
			doc->size = docLen;
			if ((stat = storeDoc (docHndl, doc, sizeof(Doc) + docLen, &docId)))
			  fprintf(stderr, "Add Doc %s Error %d Line: %" PRIu64 "\n", args->inFile, stat, line), exit(0);
		  } else
			docId.bits = line;

		  if (idxHndl->hndlBits) {
			uint32_t sfxLen = store64 (key, keyLen, docId.bits);

			switch((stat = insertKey (idxHndl, key, keyLen, sfxLen))) {
			  case DB_ERROR_unique_key_constraint:
				fprintf(stderr, "Duplicate key <%.*s> line: %" PRIu64 "\n", keyLen, key, line);
				break;
			  case DB_OK:
				break;
			  default:
			    fprintf(stderr, "thrd:%d Insert Key %s Error %d Line: %" PRIu64 "\n", args->idx, args->inFile, stat, line);
				myExit(args);
			}
		  }
		}

		msgLen += sprintf_s(msg + msgLen, msgMax - msgLen - 1, "thrd:%d cmd:%c file:%s records processed: %" PRIu64 "\n", args->idx, cmd, args->inFile, line);

		break;

	case 'f':
		if( pennysort )
		  msgLen += sprintf_s(msg + msgLen, msgMax - msgLen - 1, "thrd:%d cmd:%c file:%s find random keys\n", args->idx, cmd, args->inFile);
		else
		  msgLen += sprintf_s(msg + msgLen, msgMax - msgLen - 1, "thrd:%d cmd:%c file:%s find given keys\n", args->idx, cmd, args->inFile);

		if (args->noDocs)
			parent = database;
		else {
			openDocStore(docHndl, database, "documents", (int)strlen("documents"), args->params);
			parent = docHndl;
		}

		if ((stat = createIndex(idxHndl, parent, idxName, (int)strlen(idxName), args->params)))
		  fprintf(stderr, "createIndex %s Error %d type: %s\n", args->inFile, stat, idxName), exit(0);

		createCursor (cursor, idxHndl, args->params);

		if (binaryFlds)
			len = 2;

		if((!fopen_s (&in, args->inFile, "r"))) {
		  while( ch = getc_unlocked(in), ch != EOF )
			if( ch == '\t' || ch == '\n' ) {
			  if (binaryFlds) {
				key[lastFld] = (len - lastFld - 2) >> 8;
				key[lastFld + 1] = (len - lastFld - 2);
			  }

			  line++;

			  if (debug && !(line % 1000000))
				fprintf (stderr, "thrd:%d cmd:%c line: %"   PRIu64 "\n", args->idx, cmd, line);

			  len = args->keyLen;

			  if ((stat = positionCursor (cursor, OpOne, key, len)))
				fprintf(stderr, "findKey %s Error %d Syserr %d Line: %" PRIu64 "\n", args->inFile, stat, errno, line), myExit(args);

			  if ((stat = keyAtCursor (cursor, &foundKey, &foundLen)))
				fprintf(stderr, "findKey %s Error %d Syserr %d Line: %" PRIu64 "\n", args->inFile, stat, errno, line), myExit(args);

			  if (docHndl->hndlBits && idxHndl->hndlBits) {
                  docId.bits = get64(foundKey, foundLen);
                  foundLen -= size64(foundKey, foundLen);
			  }

			  if (!binaryFlds) {
			   if (foundLen != len)
				fprintf(stderr, "findKey %s Error len mismatch: Line: %" PRIu64 " keyLen: %d, foundLen: %d\n", args->inFile, line, len, foundLen), myExit(args);

			   if (memcmp(foundKey, key, foundLen))
				fprintf(stderr, "findKey %s not Found: line: %" PRIu64 " expected: %.*s \n", args->inFile, line, len, key), myExit(args);
			  }

			  cnt++;
			  len = binaryFlds ? 2 : 0;
			  lastFld = 0;
			} else if( len < 4096 ) {
				if(binaryFlds)
				if (ch == binaryFlds) {
					key[lastFld] = (len - lastFld - 2) >> 8;
					key[lastFld + 1] = (len - lastFld - 2);
					lastFld = len;
					len += 2;
					continue;
				}
				key[len++] = ch;
			}
		}

		msgLen += sprintf_s (msg + msgLen, msgMax - msgLen - 1, "thrd:%d cmd:%c file:	%s	keys processed: %" PRIu64 " keys found: %" PRIu64 "\n", args->idx, cmd, args->inFile, line, cnt);

		break;
	}

	if(docHndl->hndlBits)
		closeHandle (docHndl);
	if(idxHndl->hndlBits)
		closeHandle(idxHndl);

	closeHandle (database);
	return msgLen;
}

uint64_t index_scan(ThreadArg *args) {
uint64_t line = 0, cnt = 0;
DbHandle database[1];
DbHandle iterator[1];
DbHandle docHndl[1];
DbHandle cursor[1];
DbHandle idxHndl[1];
DbHandle *parent;

int msgLen = 0;

uint8_t rec[4096];
Doc *doc = (Doc *)rec;
uint8_t *key = rec + sizeof(Doc);

int len = 0;
char cmd;
uint32_t foundLen = 0;
uint32_t prevLen = 0;
bool reverse = false;
bool cntOnly = false;
bool verify = false;
int suffixLen = 0;
int lastFld = 0;
uint8_t *foundKey;
ObjId docId;
int stat;

	cloneHandle(database, args->database);
	fprintf(stderr, "\nIndex %s summary scan:\n", idxName);

	iterator->hndlBits = 0;
	docHndl->hndlBits = 0;
	cursor->hndlBits = 0;
	idxHndl->hndlBits = 0;
	docHndl->hndlBits = 0;

	while( (cmd = *args->scanCmd++) )
	  switch(cmd | 0x20) {

	  case 'i':
		fprintf(stderr," iterator scan\n");

		if (args->noDocs)
		  fprintf(stderr, "Cannot specify noDocs with iterator scan\n"), exit(0);

		if ((stat = openDocStore(docHndl, database, "documents", (int)strlen("documents"), args->params)))
		  fprintf(stderr, "openDocStore Error %d\n", stat), exit(0);

		if ((stat = createIterator(iterator, docHndl, args->params)))
		  fprintf(stderr, "createIterator Error %d\n", stat), exit(0);

		while ((moveIterator(iterator, IterNext, (void **)&doc, &docId) == DB_OK)) {
            fwrite_unlocked (doc + 1, doc->size, 1, stdout);
            fputc_unlocked ('\n', stdout);
            cnt++;
		}

		fprintf(stderr, " iterator scan total: %" PRIu64 "\n",cnt);

		if(iterator->hndlBits)
		  closeHandle (iterator);
		
		if(docHndl->hndlBits)
		  closeHandle (docHndl);
		
		if(database->hndlBits)
		  closeHandle (database);
		
		return cnt;

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

	if( verify )
	  fprintf(stderr, " key order verification\n");

	if( cntOnly )
	  fprintf(stderr, " index key count\n");
	else
	  fprintf_s(stderr, " doc/key dump\n");

	if (args->minKey)
	  fprintf(stderr, " min key: <%s>\n", args->minKey);

	if (args->maxKey)
	  fprintf(stderr, " max key: <%s>\n", args->maxKey);

	if (args->noDocs)
		parent = database;
	else {
		openDocStore(docHndl, database, "documents", (int)strlen("documents"), args->params);
		parent = docHndl;
	}

	if ((stat = createIndex(idxHndl, parent, idxName, (int)strlen(idxName), args->params)))
	  fprintf(stderr, "createIndex Error %d type: %s\n", stat, idxName), exit(0);

	// create cursor

	createCursor (cursor, idxHndl, args->params);

	if (!reverse && args->minKey)
		stat = positionCursor (cursor, OpBefore, args->minKey, (int)strlen(args->minKey));
	else if (reverse && args->maxKey)
		stat = positionCursor (cursor, OpAfter, args->maxKey, (int)strlen(args->maxKey));
	else 
		stat = moveCursor (cursor, reverse ? OpRight : OpLeft);

	if (stat)
		fprintf(stderr, "positionCursor Error %d\n", stat), exit(0);

	while (!(stat = moveCursor(cursor, reverse ? OpPrev : OpNext))) {
		int keyLen;

		if ((stat = keyAtCursor (cursor, &foundKey, &foundLen)))
		  fprintf(stderr, "keyAtCursor Error %d\n", stat), exit(0);

		keyLen = foundLen - size64(foundKey, foundLen);

		if (reverse && args->minKey)
		  if (memcmp(foundKey, args->minKey, (int)strlen(args->minKey)) < 0)
			break;
		if (!reverse && args->maxKey)
		  if (memcmp(foundKey, args->maxKey, (int)strlen(args->maxKey)) > 0)
			break;

		cnt++;

		if( numbers ) {
			uint64_t lineNo = get64(foundKey, foundLen);
			fprintf(stdout, "%.12" PRIu64 "\t", lineNo);
		}

		if( keyList )
		  if( binaryFlds)
			printBinary (binaryFlds, foundKey, keyLen, stdout);
		  else
		    fwrite_unlocked (foundKey, keyLen, 1, stdout);

		if( numbers || keyList )
			fputc_unlocked('\t', stdout);

		if (verify) {
		  if(prevLen == keyLen) {
			 int comp = memcmp (foundKey, rec, prevLen);

			 if((reverse && comp >= 0) || (!reverse && comp <= 0))
				 fprintf (stderr, "verify: Key compare error: %d\n", (int)cnt), exit (0);
		  }

		  prevLen = keyLen;
		  memcpy (rec, foundKey, keyLen);
		}

		if (cntOnly) {
		  if (debug && !(cnt % 1000000))
			fprintf (stderr, " scan %s count: %" PRIu64 "\n", idxName, cnt);
		  continue;
		}

		if (args->noDocs)
		 if (binaryFlds)
		  printBinary(binaryFlds, foundKey, keyLen, stdout);
		 else
		  fwrite_unlocked (foundKey, keyLen, 1, stdout);
		else {
		  docId.bits = get64(foundKey, foundLen);
		  fetchDoc(docHndl, (void **)&doc, docId);
		  fwrite_unlocked (doc + 1, doc->size, 1, stdout);
		}

		fputc_unlocked ('\n', stdout);
	}

	if (stat && stat != DB_CURSOR_eof)
	  fprintf(stderr, " Index %s Scan: Error %d Syserr %d Line: %" PRIu64 "\n", idxName, stat, errno, cnt), exit(0);

	fprintf(stderr, " Index scan complete \n Total keys %" PRIu64 "\n", cnt);
	prevLen = 0;
 
	if(cursor->hndlBits)
	  closeCursor (cursor);

	if(docHndl->hndlBits)
	  closeHandle (docHndl);
		
	if(idxHndl->hndlBits)
	  closeHandle (idxHndl);
		
	if(database->hndlBits)
	  closeHandle (database);

	return cnt;
}

//  run index_file once for each command

#ifndef _WIN32
void *pipego (void *arg) {
#else
unsigned __stdcall pipego (void *arg) {
#endif
ThreadArg *args = arg;
int len = 0, max, cmd;
char msg[4096];
double elapsed;

	while( (cmd = *args->cmds++) ) {
	  double startx1 = getCpuTime(0);
	  double startx2 = getCpuTime(1);
	  double startx3 = getCpuTime(2);

	  len += sprintf_s(msg + len, sizeof(msg) - len - 1, "\nthrd:%d cmd:%c %s: begin\n", args->idx, cmd, idxName);

	  max = sizeof(msg) - len - 1;
	  len += index_file(args, cmd, msg + len, max);
	  
	  elapsed = getCpuTime(0) - startx1;

	  len += sprintf_s(msg + len, sizeof(msg) - len - 1, "thrd:%d cmd:%c end\n", args->idx, cmd);

	  len += sprintf_s(msg + len, sizeof(msg) - len - 1, " real %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);

	  elapsed = getCpuTime(1) - startx2;

	  len += sprintf_s(msg + len, sizeof(msg) - len - 1, " user %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);

	  elapsed = getCpuTime(2) - startx3;

	  len += sprintf_s(msg + len, sizeof(msg) - len - 1, " sys  %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
  }

  fwrite(msg, 1ULL, len, stderr);

#ifndef _WIN32
  return NULL;
#else
  return 0;
#endif
}

int main (int argc, char **argv)
{
Params params[MaxParam];
char *summary = NULL;
char *minKey = NULL;
char *maxKey = NULL;
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

DbHandle database[1];
uint64_t totKeys;
char buf[512];
ThreadArg *args;
double elapsed;
double start;
int num = 0;

	if( argc < 3 ) {
		fprintf (stderr, "Usage: %s db_name -cmds=[wdf] -summary=[csrvi] -idxType=[012] -bits=# -xtra=# -inMem -debug -uniqueKeys -noDocs -noIdx -keyLen=# -minKey=abcd -maxKey=abce -drop -idxBinary=. -pipeline src_file1 src_file2 ... ]\n", argv[0]);
		fprintf (stderr, "  where db_name is the prefix name of the database files\n");
		fprintf (stderr, "  cmds is a string of (w)rite/(d)elete/(f)ind commands, to run sequentially on each input src_file.\n");
		fprintf (stderr, "  summary scan is a string of (c)ount/(r)ev scan/(s)can/(v)erify/(i)terate flags for a final scan after all threads have quit\n");
		fprintf (stderr, "  idxType is the type of index: 0 = ART, 1 = btree1, 2 = btree2\n");
		fprintf (stderr, "  keyLen is key size, zero for whole input file line\n");
		fprintf (stderr, "  pennysort creates random input lines, taking a count from the file name\n");
		fprintf (stderr, "  bits is the btree page size in bits\n");
		fprintf (stderr, "  xtra is the btree leaf page extra bits\n");
		fprintf (stderr, "  inMem specifies no disk files\n");
		fprintf (stderr, "  noDocs specifies keys only\n");
		fprintf (stderr, "  noIdx specifies documents only\n");
		fprintf (stderr, "  minKey specifies beginning cursor key\n");
		fprintf (stderr, "  maxKey specifies ending cursor key\n");
		fprintf (stderr, "  drop will initially drop database\n");
		fprintf (stderr, "  idxBinary utilize length counted fields separated by  the given deliminator in keys\n");
		fprintf (stderr, "  uniqueKeys ensure keys are unique\n");
		fprintf (stderr, "  run cmds in a single threaded  pipeline using one input file at a time\n");
		fprintf (stderr, "  src_file1 thru src_filen are files of keys/documents separated by newline\n");
		exit(0);
	}

	// process database name

	dbName = (++argv)[0];
	argc--;

	//	set default values

	memset (params, 0, sizeof(params));
	params[Btree1Bits].intVal = 14;
	params[OnDisk].boolVal = true;
	params[Btree2Bits].intVal = 14;

	// process configuration arguments

	while (--argc > 0 && (++argv)[0][0] == '-')
		if (!strncasecmp(argv[0], "-xtra=", 6)) {
			params[Btree1Xtra].intVal = atoi(argv[0] + 6);
			params[Btree2Xtra].intVal = atoi(argv[0] + 6);
		}
		else if (!strncasecmp(argv[0], "-keyLen=", 8))
			keyLen = atoi(argv[0] + 8);
		else if (!strncasecmp(argv[0], "-bits=", 6)) {
			params[Btree1Bits].intVal = atoi(argv[0] + 6);
			params[Btree2Bits].intVal = atoi(argv[0] + 6);
		}
		else if (!strncasecmp(argv[0], "-cmds=", 6))
			cmds = argv[0] + 6;
		else if (!strncasecmp(argv[0], "-debug", 6))
			debug = true;
		else if (!strncasecmp(argv[0], "-stats", 6))
			stats = true;
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
  else if (!strncasecmp(argv[0], "-numbers", 8))
	numbers = true;
  else if (!strncasecmp(argv[0], "-keyList", 8))
	keyList = true;
  else if (!strncasecmp(argv[0], "-uniqueKeys", 11))
	params[IdxKeyUnique].boolVal = true;
  else if (!strncasecmp(argv[0], "-idxType=", 9))
	params[IdxType].intVal = atoi(argv[0] + 9);
  else if (!strncasecmp(argv[0], "-inMem", 6))
	params[OnDisk].boolVal = false;
  else if(!strncasecmp (argv[0], "-idxBinary=", 11))
	  params[IdxKeyFlds].charVal = argv[0][11];
  else if (!strncasecmp(argv[0], "-minKey=", 8))
	minKey = argv[0] + 8;
  else if (!strncasecmp(argv[0], "-maxKey=", 8))
	maxKey = argv[0] + 8;
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
  
  start = getCpuTime(0);
  
  #ifndef _WIN32
  threads = malloc(cnt * sizeof(pthread_t));
  #else
  threads = GlobalAlloc(GMEM_FIXED | GMEM_ZEROINIT, cnt * sizeof(HANDLE));
  #endif
  args = malloc((cnt ? cnt : 1) * sizeof(ThreadArg));

	openDatabase(database, dbName, (int)strlen(dbName), params);

//  drop the database?

  if (dropDb) {
	dropArena(database, true);
	openDatabase(database, dbName, (int)strlen(dbName), params);
  }

  //	fire off threads

  idx = 0;

  do {
	args[idx].database = database;
	args[idx].inFile = idx < argc ? argv[idx] : argv[argc - 1];
	args[idx].scanCmd = summary;
	args[idx].minKey = minKey;
	args[idx].maxKey = maxKey;
	args[idx].params = params;
	args[idx].keyLen = keyLen;
	args[idx].noDocs = noDocs;
	args[idx].noExit = noExit;
	args[idx].noIdx = noIdx;
	args[idx].cmds = cmds;
	args[idx].idx = idx;

	if ( pipeLine || cnt == 1) {
		pipego(args);
		continue;
	}

#ifndef _WIN32
	if ((err = pthread_create(threads + idx, NULL, pipego, args + idx)))
		fprintf(stderr, "Error creating thread %d\n", err);
#else
	while (((int64_t)(threads[idx] = (HANDLE)_beginthreadex(NULL, 65536, pipego, args + idx, 0, NULL)) < 0LL))
		fprintf(stderr, "Error creating thread errno = %d\n", errno);
#endif

	fprintf(stderr, "thread %d launched for file %s\n", idx, args[idx].inFile);
  } while (++idx < cnt );

	// 	wait for termination

  if( !pipeLine && cnt > 1 ) {
#ifndef _WIN32
	for( idx = 0; idx < cnt; idx++ )
	  pthread_join (threads[idx], NULL);
#else
	WaitForMultipleObjects (cnt, threads, TRUE, INFINITE);

	for( idx = 0; idx < cnt; idx++ )
	  CloseHandle(threads[idx]);
#endif
  }
	if( (args->scanCmd = summary) )
		totKeys = index_scan (args);

	elapsed = getCpuTime(0) - start;

	if (stats) {
		fputc(0x0a, stderr);

		fprintf(stderr, "Total memory allocated: %.3f MB\n", (double)*totalMemoryReq/(1024. * 1024.));
		fprintf(stderr, "Bytes per key: %lld\n", *totalMemoryReq / totKeys);

		switch (params[IdxType].intVal) {
		case 0:
			fputc(0x0a, stderr);

			for (idx = 0; idx < 64; idx++)
				if (nodeAlloc[idx])
					fprintf(stderr, "%s Index type %d blks allocated: %.8lld\n", idxName, idx, nodeAlloc[idx]);

			fputc(0x0a, stderr);

			for (idx = 0; idx < 64; idx++)
				if (nodeFree[idx])
					fprintf(stderr, "%s Index type %d blks freed    : %.8lld\n", idxName, idx, nodeFree[idx]);

			fputc(0x0a, stderr);

			for (idx = 0; idx < 64; idx++)
				if (nodeWait[idx])
					fprintf(stderr, "%s Index type %d blks recycled : %.8lld\n", idxName, idx, nodeWait[idx]);

			fputc(0x0a, stderr);

			if (!pipeLine) {
				fprintf(stderr, " real %dm%.3fs\n", (int)(elapsed / 60), elapsed - (int)(elapsed / 60) * 60);
				elapsed = getCpuTime(1);
				fprintf(stderr, " user %dm%.3fs\n", (int)(elapsed / 60), elapsed - (int)(elapsed / 60) * 60);
				elapsed = getCpuTime(2);
				fprintf(stderr, " sys  %dm%.3fs\n", (int)(elapsed / 60), elapsed - (int)(elapsed / 60) * 60);
			}

			break;
		}
	}

	if(debug) {
#ifdef _WIN32
	  GetSystemInfo(info);
	  fprintf(stderr, "CWD: %s PageSize: %d, # Processors: %d, Allocation Granularity: %d\n\n", _getcwd(buf, 512), (int)info->dwPageSize, (int)info->dwNumberOfProcessors, (int)info->dwAllocationGranularity);
#endif
	}
}
