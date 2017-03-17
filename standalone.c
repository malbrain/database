#define _BSD_SOURCE
#include <errno.h>
#include <string.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>

#include "db.h"
#include "db_api.h"

#ifndef _WIN32
#include <sys/time.h>
#include <sys/resource.h>
#include <pthread.h>
#include <stdlib.h>
#else
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <process.h>
#endif

#ifdef _WIN32
#define fwrite	_fwrite_nolock
#define fputc	_fputc_nolock
#define getc	_getc_nolock
#else
#define fopen_s(file, path, mode) ((*file = fopen(path, mode)) ? 0 : errno)
#ifndef apple
#undef getc
#define fputc	fputc_unlocked
#define fwrite	fwrite_unlocked
#define getc	getc_unlocked
#endif

#endif

#ifdef DEBUG
extern uint64_t totalMemoryReq[1];
extern uint64_t nodeAlloc[64];
extern uint64_t nodeFree[64];
extern uint64_t nodeWait[64];
#endif

bool debug = false;

double getCpuTime(int type);

void printBinary(uint8_t *key, int len, FILE *file) {
int off = 0;

	while(off < len) {
		int fldLen = key[off] << 8 | key[off + 1];
		if (off)
			fputc (':', file);
		off += 2;
		fwrite (key + off, fldLen, 1, file);
		off += fldLen;
	}
}

typedef struct {
	char *cmds;
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
	int num;
} ThreadArg;

char *indexNames[] = {
"ARTreeIdx",
"Btree1Idx",
"Btree2Idx"
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
//  then list them onto std-out

#ifndef _WIN32
void *index_file (void *arg)
#else
unsigned __stdcall index_file (void *arg)
#endif
{
uint64_t line = 0, cnt = 0;
ThreadArg *args = arg;
DbHandle database[1];
DbHandle iterator[1];
DbHandle docHndl[1];
DbHandle cursor[1];
DbHandle idxHndl[1];
DbHandle *parent;

uint8_t rec[4096];
Doc *doc = (Doc *)rec;
uint8_t *key = rec + sizeof(Doc);

int ch, len = 0;
bool binaryFlds; 
char *idxName;

uint32_t foundLen = 0;
uint32_t prevLen = 0;
bool reverse = false;
bool cntOnly = false;
bool verify = false;
int suffixLen = 0;
int lastFld = 0;
void *foundKey;
ObjId docId;
FILE *in;
int stat;

	cloneHandle(database, args->database);

	iterator->hndlBits = 0;
	docHndl->hndlBits = 0;
	cursor->hndlBits = 0;
	idxHndl->hndlBits = 0;

	idxName = indexNames[args->params[IdxType].intVal];
	binaryFlds = args->params[IdxKeyFlds].boolVal;
	docHndl->hndlBits = 0;

	if( args->idx < strlen (args->cmds) )
		ch = args->cmds[args->idx];
	else
		ch = args->cmds[strlen(args->cmds) - 1];

	switch(ch | 0x20)
	{

	case 'd':
		fprintf(stderr, "started delete key for %s\n", args->inFile);
		parent = database;

		if ((stat = createIndex(idxHndl, parent, idxName, strlen(idxName), args->params)))
		  fprintf(stderr, "createIndex %s Error %d name: %s\n", args->inFile, stat, idxName), exit(0);

		createCursor (cursor, idxHndl, args->params);

		if (binaryFlds)
			len = 2;

		if( !fopen_s (&in, args->inFile, "r") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
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

			else if( len < 4096 ) {
			  if (binaryFlds)
				if (ch == ':') {
					key[lastFld] = (len - lastFld - 2) >> 8;
					key[lastFld + 1] = (len - lastFld - 2);
					lastFld = len;
					len += 2;
					continue;
				}
			  key[len++] = ch;
			}

		fprintf(stderr, "finished delete %s for %" PRIu64 " keys, found %" PRIu64 "\n", args->inFile, line, cnt);
		break;

	case 'w':
		fprintf(stderr, "started writing from %s\n", args->inFile);
		parent = database;

		if (!args->noDocs) {
		  if ((stat = openDocStore(docHndl, database, "documents", strlen("documents"), args->params)))
			fprintf(stderr, "openDocStore %s Error %d name: %s\n", args->inFile, stat, "documents"), exit(0);
		  parent = docHndl;
		}

		if (!args->noIdx)
		  if ((stat = createIndex(idxHndl, parent, idxName, strlen(idxName), args->params)))
			fprintf(stderr, "createIndex %s Error %d name: %s\n", args->inFile, stat, idxName), exit(0);

		if (args->noDocs && args->noIdx)
		  fprintf(stderr, "Cannot specify both -noDocs and -noIdx\n"), exit(0);

		len = 0;

		if (binaryFlds)
			len += 2;

		if((!fopen_s (&in, args->inFile, "r"))) {
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  if (binaryFlds) {
				key[lastFld] = (len - lastFld - 2) >> 8;
				key[lastFld + 1] = (len - lastFld - 2);
			  }

			  line++;

			  if (debug && !(line % 100000))
				fprintf(stderr, "line %" PRIu64 "\n", line);

			  // store the entry in the docStore?

			  if (docHndl->hndlBits) {
				doc->size = len;
				if ((stat = storeDoc (docHndl, doc, sizeof(Doc) + len, &docId)))
				  fprintf(stderr, "Add Doc %s Error %d Line: %" PRIu64 "\n", args->inFile, stat, line), exit(0);
			  }

			  if (len > args->keyLen)
				len = args->keyLen;

			  if (docHndl->hndlBits && idxHndl->hndlBits)
				suffixLen = store64(key, len, docId.bits, false);
 
			  if (idxHndl->hndlBits)
				switch ((stat = insertKey(idxHndl, key, len, suffixLen))) {
				  case DB_ERROR_unique_key_constraint:
					fprintf(stderr, "Duplicate key <%.*s> line: %" PRIu64 "\n", len, key, line);
					break;
				  case DB_OK:
					break;
				  default:
				    fprintf(stderr, "Insert Key %s Error %d Line: %" PRIu64 "\n", args->inFile, stat, line);
					myExit(args);
				}

			  lastFld = 0;
			  len = binaryFlds ? 2 : 0;
			}
			else if( len < 4096 ) {
			  if (binaryFlds)
				if (ch == ':') {
					key[lastFld] = (len - lastFld - 2) >> 8;
					key[lastFld + 1] = (len - lastFld - 2);
					lastFld = len;
					len += 2;
					continue;
				}
			  key[len++] = ch;
			}
		}

		fprintf(stderr, " Total records processed %" PRIu64 "\n", line);
		break;

	case 'f':
		fprintf(stderr, "started finding keys for %s\n", args->inFile);

		if (args->noDocs)
			parent = database;
		else {
			openDocStore(docHndl, database, "documents", strlen("documents"), args->params);
			parent = docHndl;
		}

		if ((stat = createIndex(idxHndl, parent, idxName, strlen(idxName), args->params)))
		  fprintf(stderr, "createIndex %s Error %d name: %s\n", args->inFile, stat, idxName), exit(0);

		createCursor (cursor, idxHndl, args->params);

		if (binaryFlds)
			len = 2;

		if((!fopen_s (&in, args->inFile, "r"))) {
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' ) {
			  if (binaryFlds) {
				key[lastFld] = (len - lastFld - 2) >> 8;
				key[lastFld + 1] = (len - lastFld - 2);
			  }

			  line++;

			  if (debug && !(line % 100000))
				fprintf(stderr, "line %" PRIu64 "\n", line);

			  len = args->keyLen;

			  if ((stat = positionCursor (cursor, OpOne, key, len)))
				fprintf(stderr, "findKey %s Error %d Syserr %d Line: %" PRIu64 "\n", args->inFile, stat, errno, line), myExit(args);

			  if ((stat = keyAtCursor (cursor, &foundKey, &foundLen)))
				fprintf(stderr, "findKey %s Error %d Syserr %d Line: %" PRIu64 "\n", args->inFile, stat, errno, line), myExit(args);

			  if (docHndl->hndlBits && idxHndl->hndlBits)
				foundLen -= get64(foundKey, foundLen, &docId.bits, binaryFlds);

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
			  if (binaryFlds)
				if (ch == ':') {
					key[lastFld] = (len - lastFld - 2) >> 8;
					key[lastFld + 1] = (len - lastFld - 2);
					lastFld = len;
					len += 2;
					continue;
				}
			  key[len++] = ch;
			}
		}

		fprintf(stderr, "finished %s for %" PRIu64 " keys, found %" PRIu64 "\n", args->inFile, line, cnt);
		break;

	case 'i':
		fprintf(stderr, "started iterator scan\n");

		if (args->noDocs)
		  fprintf(stderr, "Cannot specify noDocs with iterator scan\n"), exit(0);

		if ((stat = openDocStore(docHndl, database, "documents", strlen("documents"), args->params)))
		  fprintf(stderr, "openDocStore Error %d\n", stat), exit(0);

		if ((stat = createIterator(iterator, docHndl, args->params)))
		  fprintf(stderr, "createIterator Error %d\n", stat), exit(0);

		while ((moveIterator(iterator, IterNext, (void **)&doc, &docId) == DB_OK)) {
            fwrite (doc + 1, doc->size, 1, stdout);
            fputc ('\n', stdout);
            cnt++;
		}

		fprintf(stderr, " Total docs read %" PRIu64 "\n", cnt);
		break;

	case 'v':
		verify = true;
	case 'c':
		cntOnly = true;
	case 'r':
		reverse = true;
	case 's':
		if (reverse)
			fprintf(stderr, "started reverse cursor");
		else
			fprintf(stderr, "started forward cursor");

		if (verify)
			fprintf(stderr, " key ordering verification");

		if (cntOnly)
			fprintf(stderr, " count Only");

		if (args->minKey)
			fprintf(stderr, " min key: %s", args->minKey);

		if (args->maxKey)
			fprintf(stderr, " max key: %s", args->maxKey);

		fprintf(stderr, "\n");

		if (args->noDocs)
			parent = database;
		else {
			openDocStore(docHndl, database, "documents", strlen("documents"), args->params);
			parent = docHndl;
		}

		if ((stat = createIndex(idxHndl, parent, idxName, strlen(idxName), args->params)))
		  fprintf(stderr, "createIndex Error %d name: %s\n", stat, idxName), exit(0);

		// create cursor

		createCursor (cursor, idxHndl, args->params);

		if (!reverse && args->minKey)
			stat = positionCursor (cursor, OpBefore, args->minKey, strlen(args->minKey));
		else if (reverse && args->maxKey)
			stat = positionCursor (cursor, OpAfter, args->maxKey, strlen(args->maxKey));
		else 
			stat = moveCursor (cursor, reverse ? OpRight : OpLeft);

		if (stat)
			fprintf(stderr, "positionCursor Error %d\n", stat), exit(0);

		while (!(stat = moveCursor(cursor, reverse ? OpPrev : OpNext))) {
			if ((stat = keyAtCursor (cursor, &foundKey, &foundLen)))
			  fprintf(stderr, "keyAtCursor Error %d\n", stat), exit(0);

			if (reverse && args->minKey)
			  if (memcmp(foundKey, args->minKey, strlen(args->minKey)) < 0)
				break;
			if (!reverse && args->maxKey)
			  if (memcmp(foundKey, args->maxKey, strlen(args->maxKey)) > 0)
				break;

			cnt++;

			if (verify) {
			  if(prevLen && prevLen == foundLen)
			   if (memcmp(foundKey, rec, prevLen) > 0)
			  	fprintf(stderr, "verifyKey compare error: %d\n", (int)cnt), exit(0);

			 prevLen = foundLen;
			 memcpy (rec, foundKey, foundLen);
			}

			if (cntOnly) {
			  if (debug && !(cnt % 100000))
				fprintf(stderr, "key %" PRIu64 "\n", cnt);

			  continue;
			}

			if (args->noDocs)
			 if (binaryFlds)
			  printBinary(foundKey, foundLen, stdout);
			 else
			  fwrite (foundKey, foundLen, 1, stdout);
			else {
			  get64(foundKey, foundLen, &docId.bits, false);
			  fetchDoc(docHndl, (void **)&doc, docId);
			  fwrite (doc + 1, doc->size, 1, stdout);
			}

			fputc ('\n', stdout);
		}

		if (stat && stat != DB_CURSOR_eof)
		  fprintf(stderr, "Scan: Error %d Syserr %d Line: %" PRIu64 "\n", stat, errno, cnt), exit(0);

		fprintf(stderr, " Total keys %" PRIu64 "\n", cnt);
		break;
	}

	if (iterator->hndlBits)
		closeHandle(iterator);
	if (docHndl->hndlBits)
		closeHandle(docHndl);
	if (cursor->hndlBits)
		closeHandle(cursor);
	if (idxHndl->hndlBits)
		closeHandle(idxHndl);

	closeHandle(database);

#ifndef _WIN32
	return NULL;
#else
	return 0;
#endif
}

typedef struct timeval timer;

int main (int argc, char **argv)
{
Params params[MaxParam];
int idx, cnt, err;
char *minKey = NULL;
char *maxKey = NULL;
char *dbName = NULL;
char *cmds = NULL;
int keyLen = 10;

#ifndef _WIN32
pthread_t *threads;
#else
SYSTEM_INFO info[1];
HANDLE *threads;
#endif

DbHandle database[1];

bool dropDb = false;
bool noExit = false;
bool noDocs = false;
bool noIdx = false;

ThreadArg *args;
float elapsed;
double start;
int num = 0;

#ifdef _WIN32
	GetSystemInfo(info);
	fprintf(stderr, "PageSize: %d, # Processors: %d, Allocation Granularity: %d\n\n", (int)info->dwPageSize, (int)info->dwNumberOfProcessors, (int)info->dwAllocationGranularity);
#endif
	if( argc < 3 ) {
		fprintf (stderr, "Usage: %s db_name -cmds=[crwsdfiv]... -idxType=[012] -bits=# -xtra=# -inMem -debug -uniqueKeys -noDocs -noIdx -keyLen=# -minKey=abcd -maxKey=abce -drop -idxBinary src_file1 src_file2 ... ]\n", argv[0]);
		fprintf (stderr, "  where db_name is the prefix name of the database file\n");
		fprintf (stderr, "  cmds is a string of (c)ount/(r)ev scan/(w)rite/(s)can/(d)elete/(f)ind/(v)erify/(i)terate, with a one character command for each input src_file, or a no-input command.\n");
		fprintf (stderr, "  idxType is the type of index: 0 = ART, 1 = btree1, 2 = btree2\n");
		fprintf (stderr, "  keyLen is key size, zero for whole line\n");
		fprintf (stderr, "  bits is the btree page size in bits\n");
		fprintf (stderr, "  xtra is the btree leaf page extra bits\n");
		fprintf (stderr, "  inMem specifies no disk files\n");
		fprintf (stderr, "  noDocs specifies keys only\n");
		fprintf (stderr, "  noIdx specifies documents only\n");
		fprintf (stderr, "  minKey specifies beginning cursor key\n");
		fprintf (stderr, "  maxKey specifies ending cursor key\n");
		fprintf (stderr, "  drop will initially drop database\n");
		fprintf (stderr, "  idxBinary utilize length counted fields\n");
		fprintf (stderr, "  uniqueKeys ensure keys are unique\n");
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

	// process configuration arguments

	while (--argc > 0 && (++argv)[0][0] == '-')
	  if (!memcmp(argv[0], "-xtra=", 6))
			params[Btree1Xtra].intVal = atoi(argv[0] + 6);
	  else if (!memcmp(argv[0], "-keyLen=", 8))
			keyLen = atoi(argv[0] + 8);
	  else if (!memcmp(argv[0], "-bits=", 6))
			params[Btree1Bits].intVal = atoi(argv[0] + 6);
	  else if (!memcmp(argv[0], "-cmds=", 6))
			cmds = argv[0] + 6;
	  else if (!memcmp(argv[0], "-debug", 6))
			debug = true;
	  else if (!memcmp(argv[0], "-drop", 5))
			dropDb = true;
	  else if (!memcmp(argv[0], "-noIdx", 6))
			noIdx = true;
	  else if (!memcmp(argv[0], "-noDocs", 7))
			noDocs = true;
	  else if (!memcmp(argv[0], "-noExit", 7))
			noExit = true;
	  else if (!memcmp(argv[0], "-uniqueKeys", 11))
			params[IdxKeyUnique].boolVal = true;
	  else if (!memcmp(argv[0], "-idxType=", 9))
			params[IdxType].intVal = atoi(argv[0] + 9);
	  else if (!memcmp(argv[0], "-inMem", 6))
			params[OnDisk].boolVal = false;
	  else if (!memcmp(argv[0], "-idxBinary", 10))
			params[IdxKeyFlds].boolVal = true;
	  else if (!memcmp(argv[0], "-minKey=", 8))
			minKey = argv[0] + 8;
	  else if (!memcmp(argv[0], "-maxKey=", 8))
			maxKey = argv[0] + 8;
	  else
			fprintf(stderr, "Unknown option %s ignored\n", argv[0]);

	cnt = argc;

	initialize();

	start = getCpuTime(0);

#ifndef _WIN32
	threads = malloc (cnt * sizeof(pthread_t));
#else
	threads = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, cnt * sizeof(HANDLE));
#endif
	args = malloc ((cnt ? cnt : 1) * sizeof(ThreadArg));

	openDatabase(database, dbName, strlen(dbName), params);

	//  drop the database?

	if (dropDb) {
		dropArena(database, true);
		openDatabase(database, dbName, strlen(dbName), params);
	}

	//	fire off threads

	idx = 0;

	do {
	  args[idx].database = database;
	  args[idx].inFile = argv[idx];
	  args[idx].minKey = minKey;
	  args[idx].maxKey = maxKey;
	  args[idx].params = params;
	  args[idx].keyLen = keyLen;
	  args[idx].noDocs = noDocs;
	  args[idx].noExit = noExit;
	  args[idx].noIdx = noIdx;
	  args[idx].cmds = cmds;
	  args[idx].num = num;
	  args[idx].idx = idx;

	  if (cnt > 1) {
#ifndef _WIN32
		if((err = pthread_create (threads + idx, NULL, index_file, args + idx)))
		  fprintf(stderr, "Error creating thread %d\n", err);
#else
		while (((int64_t)(threads[idx] = (HANDLE)_beginthreadex(NULL, 65536, index_file, args + idx, 0, NULL)) < 0LL))
		  fprintf(stderr, "Error creating thread errno = %d\n", errno);

#endif
		continue;
	  } else
	  	//  if zero or one files specified,
	  	//  run index_file once

	  	index_file (args);
	} while (++idx < cnt);

	// 	wait for termination

#ifndef _WIN32
	if (cnt > 1)
	  for( idx = 0; idx < cnt; idx++ )
		pthread_join (threads[idx], NULL);
#else
	if (cnt > 1)
	  WaitForMultipleObjects (cnt, threads, TRUE, INFINITE);

	if (cnt > 1)
	  for( idx = 0; idx < cnt; idx++ )
		CloseHandle(threads[idx]);
#endif

	elapsed = getCpuTime(0) - start;

#ifdef DEBUG
	fputc(0x0a, stderr);

	fprintf(stderr, "Total memory allocated: %lld\n", *totalMemoryReq);

	fputc(0x0a, stderr);

	for (idx = 0; idx < 64; idx++)
	  if (nodeAlloc[idx])
		fprintf(stderr, "Index type %d blks allocated: %.8lld\n", idx, nodeAlloc[idx]);

	fputc(0x0a, stderr);

	for (idx = 0; idx < 64; idx++)
	  if (nodeFree[idx])
		fprintf(stderr, "Index type %d blks freed    : %.8lld\n", idx, nodeFree[idx]);

	fputc(0x0a, stderr);

	for (idx = 0; idx < 64; idx++)
	  if (nodeWait[idx])
		fprintf(stderr, "Index type %d blks recycled : %.8lld\n", idx, nodeWait[idx]);

	fputc(0x0a, stderr);
#endif

	fprintf(stderr, " real %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	elapsed = getCpuTime(1);
	fprintf(stderr, " user %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	elapsed = getCpuTime(2);
	fprintf(stderr, " sys  %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);

}
