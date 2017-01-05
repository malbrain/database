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

double getCpuTime(int type);

//  Interface function to evaluate a partial document

bool evalPartial(Ver *ver, Params *params) {
	return true;
}

//  Interface function to create a document key
//	from a document and a key length

uint16_t keyGenerator (char *key, Ver *ver, ParamVal *spec, Params *params) {
uint16_t keyLen;

	if (!(keyLen = params[IdxKeySpec].intVal))
		keyLen = ver->size;

	memcpy(key, ver + 1, keyLen);
	return keyLen;
}

void printBinary(char *key, int len, FILE *file) {
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

DbAddr compileKeys(DbMap *map, Params *params) {
DbAddr addr;

	addr.bits = 0;
	return addr;
}

typedef struct {
	char *cmds;
	uint8_t idx;
	char *inFile;
	char *minKey;
	char *maxKey;
	Params *params;
	DbHandle *database;
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
DbHandle docHndl[1];
DbHandle cursor[1];
DbHandle index[1];
DbHandle *parent;
int ch, len = 0;
bool binaryFlds; 
char key[4096];
char *idxName;

uint32_t foundLen = 0;
int lastFld = 0;
void *foundKey;
ObjId docId;
ObjId txnId;
Doc *doc;
FILE *in;
int stat;

	cloneHandle(database, args->database);

	idxName = indexNames[args->params[IdxType].intVal];
	binaryFlds = args->params[IdxBinary].boolVal;
	docHndl->hndlBits = 0;
	txnId.bits = 0;

	if( args->idx < strlen (args->cmds) )
		ch = args->cmds[args->idx];
	else
		ch = args->cmds[strlen(args->cmds) - 1];

	switch(ch | 0x20)
	{

	case 'd':
		fprintf(stderr, "started delete for %s\n", args->inFile);

		if (args->params[NoDocs].boolVal)
			parent = database;
		else {
			openDocStore(docHndl, database, "documents", strlen("documents"), args->params);
			parent = docHndl;
		}

		if ((stat = createIndex(index, parent, idxName, strlen(idxName), args->params)))
		  fprintf(stderr, "createIndex Error %d name: %s\n", stat, idxName), exit(0);

		if (docHndl->hndlBits)
			addIndexes(docHndl);

		createCursor (cursor, index, args->params);

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
#ifdef DEBUG
			  if (!(line % 100000))
				fprintf(stderr, "line %" PRIu64 "\n", line);
#endif
			  line++;

			  if (docHndl->hndlBits) {
				if ((stat = positionCursor(cursor, OpFind, key,  len)))
				  fprintf(stderr, "Delete Doc Error %d Syserr %d Line: %" PRIu64 "\n", stat, errno, line), exit(0);

				if ((stat = docAtCursor(cursor, &doc)))
				  fprintf(stderr, "Delete Doc Error %d Syserr %d Line: %" PRIu64 "\n", stat, errno, line), exit(0);
				if ((stat = deleteDoc (docHndl, doc->ver->docId, txnId)))
				  fprintf(stderr, "Del Doc Error %d Line: %" PRIu64 "\n", stat, line), exit(0);
			  } else
				if ((stat = deleteKey(index, key, len)))
				  fprintf(stderr, "Delete Key Error %d Line: %" PRIu64 "\n", stat, line), exit(0);

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

		fprintf(stderr, "finished %s for %" PRIu64 " keys, found %" PRIu64 "\n", args->inFile, line, cnt);
		break;

	case 'w':
		fprintf(stderr, "started indexing keys from %s\n", args->inFile);

		if (args->params[NoDocs].boolVal)
			parent = database;
		else {
			openDocStore(docHndl, database, "documents", strlen("documents"), args->params);
			parent = docHndl;
		}

		if ((stat = createIndex(index, parent, idxName, strlen(idxName), args->params)))
		  fprintf(stderr, "createIndex Error %d name: %s\n", stat, idxName), exit(0);

		if (docHndl->hndlBits)
			addIndexes(docHndl);

		if (binaryFlds)
			len = 2;

		if((!fopen_s (&in, args->inFile, "r"))) {
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  if (binaryFlds) {
				key[lastFld] = (len - lastFld - 2) >> 8;
				key[lastFld + 1] = (len - lastFld - 2);
			  }
#ifdef DEBUG
			  if (!(line % 100000))
				fprintf(stderr, "line %" PRIu64 "\n", line);
#endif
			  line++;

			  if (docHndl->hndlBits) {
				if ((stat = storeDoc (docHndl, key, len, &docId, txnId)))
				  fprintf(stderr, "Add Doc Error %d Line: %" PRIu64 "\n", stat, line), exit(0);
			  } else
				if ((stat = insertKey(index, key, len)))
				  fprintf(stderr, "Insert Key Error %d Line: %" PRIu64 "\n", stat, line), exit(0);
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

		fprintf(stderr, " Total keys indexed %" PRIu64 "\n", line);
		break;

	case 'f':
		fprintf(stderr, "started finding keys for %s\n", args->inFile);

		if (args->params[NoDocs].boolVal)
			parent = database;
		else {
			openDocStore(docHndl, database, "documents", strlen("documents"), args->params);
			parent = docHndl;
		}

		if ((stat = createIndex(index, parent, idxName, strlen(idxName), args->params)))
		  fprintf(stderr, "createIndex Error %d name: %s\n", stat, idxName), exit(0);

		if (docHndl->hndlBits)
			addIndexes(docHndl);

		createCursor (cursor, index, args->params);

		if (binaryFlds)
			len = 2;

		if((!fopen_s (&in, args->inFile, "r"))) {
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' ) {
			  if (binaryFlds) {
				key[lastFld] = (len - lastFld - 2) >> 8;
				key[lastFld + 1] = (len - lastFld - 2);
			  }
#ifdef DEBUG
			  if (!(line % 100000))
				fprintf(stderr, "line %" PRIu64 "\n", line);
#endif
			  line++;

			  if (args->params[IdxKeySpec].intVal)
				len = args->params[IdxKeySpec].intVal;

			  if ((stat = positionCursor (cursor, OpOne, key, len)))
				fprintf(stderr, "findKey Error %d Syserr %d Line: %" PRIu64 "\n", stat, errno, line), exit(0);

			  if ((stat = keyAtCursor (cursor, &foundKey, &foundLen)))
				fprintf(stderr, "findKey Error %d Syserr %d Line: %" PRIu64 "\n", stat, errno, line), exit(0);

			  if (!binaryFlds) {
			   if (foundLen != len)
				fprintf(stderr, "findKey Error len mismatch: Line: %" PRIu64 " keyLen: %d, foundLen: %d\n", line, len, foundLen), exit(0);

			   if (memcmp(foundKey, key, foundLen))
				fprintf(stderr, "findKey not Found: line: %" PRIu64 " expected: %.*s \n", line, len, key), exit(0);
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

	case 's':
		fprintf(stderr, "started scanning");

		if (args->minKey)
			fprintf(stderr, " min key: %s", args->minKey);

		if (args->maxKey)
			fprintf(stderr, " max key: %s", args->maxKey);

		fprintf(stderr, "\n");

		if (args->params[NoDocs].boolVal)
			parent = database;
		else {
			openDocStore(docHndl, database, "documents", strlen("documents"), args->params);
			parent = docHndl;
		}

		if ((stat = createIndex(index, parent, idxName, strlen(idxName), args->params)))
		  fprintf(stderr, "createIndex Error %d name: %s\n", stat, idxName), exit(0);

		if (docHndl->hndlBits)
			addIndexes(docHndl);

		// create forward cursor

		createCursor (cursor, index, args->params);

		if (args->maxKey)
			setCursorMax (cursor, args->maxKey, strlen(args->maxKey));

		if (args->minKey)
			setCursorMin (cursor, args->minKey, strlen(args->minKey));

		if ((stat = moveCursor (cursor, OpLeft)))
			fprintf(stderr, "positionCursor OpLeft Error %d\n", stat), exit(0);

		if (args->params[NoDocs].boolVal)
		  while (!(stat = moveCursor(cursor, OpNext))) {
			if ((stat = keyAtCursor (cursor, &foundKey, &foundLen)))
			  fprintf(stderr, "keyAtCursor Error %d\n", stat), exit(0);

			if (binaryFlds)
			  printBinary(foundKey, foundLen, stdout);
			else
			  fwrite (foundKey, foundLen, 1, stdout);

			fputc ('\n', stdout);
			cnt++;
		  }
		else
		  while (!(stat = nextDoc(cursor, &doc))) {
            fwrite (doc + 1, doc->size, 1, stdout);
            fputc ('\n', stdout);
            cnt++;
		  }

		if (stat != DB_CURSOR_eof)
		  fprintf(stderr, "fwdScan: Error %d Syserr %d Line: %" PRIu64 "\n", stat, errno, cnt), exit(0);

		fprintf(stderr, " Total keys read %" PRIu64 "\n", cnt);
		break;

	case 'r':
		fprintf(stderr, "started reverse scanning");

		if (args->minKey)
			fprintf(stderr, " min key: %s", args->minKey);

		if (args->maxKey)
			fprintf(stderr, " max key: %s", args->maxKey);

		fprintf(stderr, "\n");

		if (args->params[NoDocs].boolVal)
			parent = database;
		else {
			openDocStore(docHndl, database, "documents", strlen("documents"), args->params);
			parent = docHndl;
		}

		if ((stat = createIndex(index, parent, idxName, strlen(idxName), args->params)))
		  fprintf(stderr, "createIndex Error %d name: %s\n", stat, idxName), exit(0);

		if (docHndl->hndlBits)
			addIndexes(docHndl);

		// create reverse cursor

		createCursor (cursor, index, args->params);

		if (args->maxKey)
			setCursorMax (cursor, args->maxKey, strlen(args->maxKey));

		if (args->minKey)
			setCursorMin (cursor, args->minKey, strlen(args->minKey));

		if ((stat = moveCursor (cursor, OpRight)))
			fprintf(stderr, "positionCursor OpRight Error %d\n", stat), exit(0);

		if (args->params[NoDocs].boolVal)
		  while (!(stat = moveCursor(cursor, OpPrev))) {
			if ((stat = keyAtCursor (cursor, &foundKey, &foundLen)))
			  fprintf(stderr, "keyAtCursor Error %d\n", stat), exit(0);
			fwrite (foundKey, foundLen, 1, stdout);
			fputc ('\n', stdout);
			cnt++;
		  }
		else
		  while (!(stat = prevDoc(cursor, &doc))) {
            fwrite (doc + 1, doc->size, 1, stdout);
            fputc ('\n', stdout);
            cnt++;
		  }

		if (stat != DB_CURSOR_eof)
		  fprintf(stderr, "revScan: Error %d Syserr %d Line: %" PRIu64 "\n", stat, errno, cnt), exit(0);

		fprintf(stderr, " Total keys read %" PRIu64 "\n", cnt);
		break;

	case 'c':
		fprintf(stderr, "started counting");

		if (args->minKey)
			fprintf(stderr, " min key: %s", args->minKey);

		if (args->maxKey)
			fprintf(stderr, " max key: %s", args->maxKey);

		fprintf(stderr, "\n");

		if (args->params[NoDocs].boolVal)
			parent = database;
		else {
			openDocStore(docHndl, database, "documents", strlen("documents"), args->params);
			parent = docHndl;
		}

		if ((stat = createIndex(index, parent, idxName, strlen(idxName), args->params)))
		  fprintf(stderr, "createIndex Error %d name: %s\n", stat, idxName), exit(0);

		//  create forward cursor

		createCursor (cursor, index, args->params);

		if (args->maxKey)
			setCursorMax (cursor, args->maxKey, strlen(args->maxKey));

		if (args->minKey)
			setCursorMin (cursor, args->minKey, strlen(args->minKey));

		if ((stat = moveCursor (cursor, OpLeft)))
			fprintf(stderr, "positionCursor OpLeft Error %d\n", stat), exit(0);

		if (args->params[NoDocs].boolVal)
	  	  while (!(stat = moveCursor(cursor, OpNext)))
			cnt++;
		else
	  	  while (!(stat = nextDoc(cursor, &doc)))
			cnt++;

		fprintf(stderr, " Total keys counted %" PRIu64 "\n", cnt);
		break;
	}

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
int idxType = 0;
char *minKey = NULL;
char *maxKey = NULL;
char *dbName = NULL;
char *cmds = NULL;

#ifndef _WIN32
pthread_t *threads;
#else
SYSTEM_INFO info[1];
HANDLE *threads;
#endif

DbHandle database[1];
ThreadArg *args;
float elapsed;
double start;
int num = 0;

#ifdef _WIN32
	GetSystemInfo(info);
	fprintf(stderr, "PageSize: %d, # Processors: %d, Allocation Granularity: %d\n\n", (int)info->dwPageSize, (int)info->dwNumberOfProcessors, (int)info->dwAllocationGranularity);
#endif
	if( argc < 3 ) {
		fprintf (stderr, "Usage: %s db_name -cmds=[crwsdf]... -idxType=[012] -bits=# -xtra=# -inMem -txns -noDocs -keyLen=# -minKey=abcd -maxKey=abce -drop -idxBinary src_file1 src_file2 ... ]\n", argv[0]);
		fprintf (stderr, "  where db_name is the prefix name of the database file\n");
		fprintf (stderr, "  cmds is a string of (c)ount/(r)ev scan/(w)rite/(s)can/(d)elete/(f)ind, with a one character command for each input src_file, or a no-input command.\n");
		fprintf (stderr, "  idxType is the type of index: 0 = ART, 1 = btree1, 2 = btree2\n");
		fprintf (stderr, "  keyLen is key size, zero for whole line\n");
		fprintf (stderr, "  bits is the btree page size in bits\n");
		fprintf (stderr, "  xtra is the btree leaf page extra bits\n");
		fprintf (stderr, "  inMem specifies no disk files\n");
		fprintf (stderr, "  noDocs specifies keys only\n");
		fprintf (stderr, "  txns indicates use of transactions\n");
		fprintf (stderr, "  minKey specifies beginning cursor key\n");
		fprintf (stderr, "  maxKey specifies ending cursor key\n");
		fprintf (stderr, "  drop will initially drop database\n");
		fprintf (stderr, "  idxBinary utilize length counted fields\n");
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
	params[IdxKeySpec].intVal = 10;

	// process configuration arguments

	while (--argc > 0 && (++argv)[0][0] == '-')
	  if (!memcmp(argv[0], "-xtra=", 6))
			params[Btree1Xtra].intVal = atoi(argv[0] + 6);
	  else if (!memcmp(argv[0], "-keyLen=", 8))
			params[IdxKeySpec].intVal = atoi(argv[0] + 8);
	  else if (!memcmp(argv[0], "-bits=", 6))
			params[Btree1Bits].intVal = atoi(argv[0] + 6);
	  else if (!memcmp(argv[0], "-cmds=", 6))
			cmds = argv[0] + 6;
	  else if (!memcmp(argv[0], "-idxType=", 9))
			params[IdxType].intVal = atoi(argv[0] + 9);
	  else if (!memcmp(argv[0], "-inMem", 6))
			params[OnDisk].boolVal = false;
	  else if (!memcmp(argv[0], "-drop", 5))
			params[DropDb].boolVal = true;
	  else if (!memcmp(argv[0], "-idxBinary", 10))
			params[IdxBinary].boolVal = true;
	  else if (!memcmp(argv[0], "-txns", 5))
			params[UseTxn].boolVal = true;
	  else if (!memcmp(argv[0], "-noDocs", 7))
			params[NoDocs].boolVal = true;
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

	if (params[DropDb].boolVal) {
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
