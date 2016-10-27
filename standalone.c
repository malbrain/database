#include <errno.h>
#include <string.h>

#include "db.h"
#include "db_api.h"

#ifndef _WIN32
#include <sys/time.h>
#include <sys/resource.h>
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
#undef getc
#define fputc	fputc_unlocked
#define fwrite	fwrite_unlocked
#define getc	getc_unlocked
#endif

double getCpuTime(int type);

//  Interface function to evaluate a partial document

bool partialEval(Document *doc, Object *spec) {
	return true;
}

//  Interface function to create a document key
//	from a document and a key length

typedef struct {
	int keyLen;
} KeySpec;

uint16_t keyGenerator (uint8_t *key, Document *doc, Object *spec) {
KeySpec *keySpec = (KeySpec *)(spec + 1);
uint16_t keyLen = 0;

	if (!(keyLen = keySpec->keyLen))
		keyLen = doc->size;

	memcpy(key, doc + 1, keyLen);
	return keyLen;
}

typedef struct {
	char idx;
	char *cmds;
	char *inFile;
	char *maxKey;
	char *minKey;
	Params *params;
	DbHandle *database;
	int num, idxType, keyLen;
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

#ifdef unix
void *index_file (void *arg)
#else
unsigned __stdcall index_file (void *arg)
#endif
{
uint64_t line = 0, cnt = 0;
unsigned char key[4096];
int ch, len = 0, slot;
ThreadArg *args = arg;
HandleType idxType;
DbHandle database[1];
DbHandle docHndl[1];
DbHandle cursor[1];
DbHandle index[1];
DbHandle *parent;
char *idxName;
bool found;

uint32_t maxKeyLen = 0;
uint32_t minKeyLen = 0;
uint32_t foundLen = 0;
uint8_t *foundKey;
Document *doc;
int idx, stat;
ObjId objId;
ObjId txnId;
FILE *in;

	cloneHandle(database, args->database);

	if (args->maxKey)
		maxKeyLen = strlen(args->maxKey);
	
	if (args->minKey)
		minKeyLen = strlen(args->minKey);
	
	idxType = indexType[args->idxType];
	idxName = indexNames[args->idxType];
	*docHndl->handle = 0;

	txnId.bits = 0;

	if( args->idx < strlen (args->cmds) )
		ch = args->cmds[args->idx];
	else
		ch = args->cmds[strlen(args->cmds) - 1];

	switch(ch | 0x20)
	{
/*
	case 'd':
		fprintf(stderr, "started delete for %s\n", args->inFile);

		if (args->params[NoDocs].boolVal)
			parent = database;
		else {
			openDocStore(docHndl, database, "documents", strlen("documents"), args->params);
			parent = docHndl;
		}

		if ((stat = createIndex(index, parent, idxType, idxName, strlen(idxName), args->params)))
		  fprintf(stderr, "createIndex Error %d name: %s\n", stat, idxName), exit(0);

		if (docHndl->handle->bits)
			addIndexes(docHndl);

		if( in = fopen (args->inFile, "rb") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
#ifdef DEBUG
			  if (!(line % 100000))
				fprintf(stderr, "line %lld\n", line);
#endif
			  line++;

			  if ((stat = delDocument (docHndl, key, len, &objId, txnId)))
				  fprintf(stderr, "Del Document Error %d Line: %lld\n", stat, line), exit(0);
			  len = 0;
			  continue;
			}

			else if( len < 4096 )
				key[len++] = ch;

		fprintf(stderr, "finished %s for %d keys: %d reads %d writes %d found\n", args->inFile, line, bt->reads, bt->writes, bt->found);
		break;
*/

	case 'w':
		fprintf(stderr, "started indexing for %s\n", args->inFile);

		if (args->params[NoDocs].boolVal)
			parent = database;
		else {
			openDocStore(docHndl, database, "documents", strlen("documents"), args->params);
			parent = docHndl;
		}

		if ((stat = createIndex(index, parent, idxType, idxName, strlen(idxName), args->params)))
		  fprintf(stderr, "createIndex Error %d name: %s\n", stat, idxName), exit(0);

		if (*docHndl->handle)
			addIndexes(docHndl);

		if( in = fopen (args->inFile, "r") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
#ifdef DEBUG
			  if (!(line % 100000))
				fprintf(stderr, "line %lld\n", line);
#endif
			  line++;

			  if (*docHndl->handle) {
				if ((stat = addDocument (docHndl, key, len, &objId, txnId)))
				  fprintf(stderr, "Add Document Error %d Line: %lld\n", stat, line), exit(0);
			  } else
				if ((stat = insertKey(index, key, len)))
				  fprintf(stderr, "Insert Key Error %d Line: %lld\n", stat, line), exit(0);
			  len = 0;
			}
			else if( len < 4096 )
				key[len++] = ch;

		fprintf(stderr, " Total keys indexed %lld\n", line);
		break;

	case 'f':
		fprintf(stderr, "started finding keys for %s\n", args->inFile);

		if (args->params[NoDocs].boolVal)
			parent = database;
		else {
			openDocStore(docHndl, database, "documents", strlen("documents"), args->params);
			parent = docHndl;
		}

		if ((stat = createIndex(index, parent, idxType, idxName, strlen(idxName), args->params)))
		  fprintf(stderr, "createIndex Error %d name: %s\n", stat, idxName), exit(0);

		if (*docHndl->handle)
			addIndexes(docHndl);

		createCursor (cursor, index, txnId, args->params);

		if( in = fopen (args->inFile, "rb") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
#ifdef DEBUG
			  if (!(line % 100000))
				fprintf(stderr, "line %lld\n", line);
#endif
			  line++;

			  if (args->keyLen)
				len = args->keyLen;

			  if ((stat = positionCursor (cursor, OpOne, key, len)))
				fprintf(stderr, "findKey Error %d Syserr %d Line: %lld\n", stat, errno, line), exit(0);

			  if ((stat = keyAtCursor (cursor, &foundKey, &foundLen)))
				fprintf(stderr, "findKey Error %d Syserr %d Line: %lld\n", stat, errno, line), exit(0);

			  if (foundLen != len)
				fprintf(stderr, "findKey Error len mismatch: Line: %lld keyLen: %d, foundLen: %d\n", line, len, foundLen), exit(0);

			  if (memcmp(foundKey, key, foundLen))
				fprintf(stderr, "findKey not Found: line: %lld expected: %.*s \n", line, len, key), exit(0);

			  cnt++;
			  len = 0;
			}
			else if( len < 4096 )
				key[len++] = ch;

		fprintf(stderr, "finished %s for %lld keys, found %lld\n", args->inFile, line, cnt);
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

		if ((stat = createIndex(index, parent, idxType, idxName, strlen(idxName), args->params)))
		  fprintf(stderr, "createIndex Error %d name: %s\n", stat, idxName), exit(0);

		if (*docHndl->handle)
			addIndexes(docHndl);

		// create forward cursor

		createCursor (cursor, index, txnId, args->params);

		if ((stat = moveCursor (cursor, OpLeft)))
			fprintf(stderr, "positionCursor OpLeft Error %d\n", stat), exit(0);

		if (args->params[NoDocs].boolVal)
		  while (!(stat = moveCursor(cursor, OpNext))) {
			if ((stat = keyAtCursor (cursor, &foundKey, &foundLen)))
			  fprintf(stderr, "keyAtCursor Error %d\n", stat), exit(0);
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
		  fprintf(stderr, "fwdScan: Error %d Syserr %d Line: %lld\n", stat, errno, cnt), exit(0);

		fprintf(stderr, " Total keys read %lld\n", cnt);
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

		if ((stat = createIndex(index, parent, idxType, idxName, strlen(idxName), args->params)))
		  fprintf(stderr, "createIndex Error %d name: %s\n", stat, idxName), exit(0);

		if (*docHndl->handle)
			addIndexes(docHndl);

		// create reverse cursor

		createCursor (cursor, index, txnId, args->params);

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
		  fprintf(stderr, "revScan: Error %d Syserr %d Line: %lld\n", stat, errno, cnt), exit(0);

		fprintf(stderr, " Total keys read %lld\n", cnt);
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

		if ((stat = createIndex(index, parent, idxType, idxName, strlen(idxName), args->params)))
		  fprintf(stderr, "createIndex Error %d name: %s\n", stat, idxName), exit(0);

		//  create forward cursor

		createCursor (cursor, index, txnId, args->params);

		if ((stat = moveCursor (cursor, OpLeft)))
			fprintf(stderr, "positionCursor OpLeft Error %d\n", stat), exit(0);

		if (args->params[NoDocs].boolVal)
	  	  while (!(stat = moveCursor(cursor, OpNext)))
			cnt++;
		else
	  	  while (!(stat = nextDoc(cursor, &doc)))
			cnt++;

		fprintf(stderr, " Total keys counted %lld\n", cnt);
		break;
	}

#ifdef unix
	return NULL;
#else
	return 0;
#endif
}

typedef struct timeval timer;

int main (int argc, char **argv)
{
int idx, cnt, len, slot, err;
Params params[MaxParam];
char *minKey = NULL;
char *maxKey = NULL;
KeySpec *keySpec;
uint64_t keyAddr;
int keyLen = 10;
int idxType = 0;
char *dbName;
char *cmds;

double start, stop;
#ifdef unix
pthread_t *threads;
#else
SYSTEM_INFO info[1];
HANDLE *threads;
#endif
ThreadArg *args;
float elapsed;
int num = 0;
char key[1];
bool onDisk = true;
DbHandle database[1];
DbHandle docHndl[1];
DbHandle index[1];

#ifdef _WIN32
	GetSystemInfo(info);
	fprintf(stderr, "PageSize: %d, # Processors: %d, Allocation Granularity: %d\n\n", info->dwPageSize, info->dwNumberOfProcessors, info->dwAllocationGranularity);
#endif
	if( argc < 3 ) {
		fprintf (stderr, "Usage: %s db_name -cmds=[crwsdf]... -idxType=[012] -bits=# -xtra=# -inMem -txns -noDocs -keyLen=# -minKey=abcd -maxKey=abce src_file1 src_file2 ... ]\n", argv[0]);
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
	  else if (!memcmp(argv[0], "-idxType=", 9))
			idxType = atoi(argv[0] + 9);
	  else if (!memcmp(argv[0], "-inMem", 6))
			params[OnDisk].boolVal = false;
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

#ifdef unix
	threads = malloc (cnt * sizeof(pthread_t));
#else
	threads = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, cnt * sizeof(HANDLE));
#endif
	args = malloc ((cnt ? cnt : 1) * sizeof(ThreadArg));

	openDatabase(database, dbName, strlen(dbName), params);

	keyAddr = arenaAlloc(database, sizeof(KeySpec), true, true);
	params[IdxKeySpec].int64Val = keyAddr;

	keySpec = (KeySpec *)(arenaObj(database, keyAddr, true) + 1);
	keySpec->keyLen = keyLen;

	//	fire off threads

	idx = 0;

	do {
	  args[idx].database = database;
	  args[idx].inFile = argv[idx];
	  args[idx].idxType = idxType;
	  args[idx].params = params;
	  args[idx].minKey = minKey;
	  args[idx].maxKey = maxKey;
	  args[idx].keyLen = keyLen;
	  args[idx].cmds = cmds;
	  args[idx].num = num;
	  args[idx].idx = idx;

	  if (cnt > 1) {
#ifdef unix
		if( err = pthread_create (threads + idx, NULL, index_file, args + idx) )
		  fprintf(stderr, "Error creating thread %d\n", err);
#else
		while ( (int64_t)(threads[idx] = (HANDLE)_beginthreadex(NULL, 65536, index_file, args + idx, 0, NULL)) < 0LL)
		  fprintf(stderr, "Error creating thread errno = %d\n", errno);

#endif
		continue;
	  } else
	  	//  if zero or one files specified,
	  	//  run index_file once

	  	index_file (args);
	} while (++idx < cnt);

	// 	wait for termination

#ifdef unix
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
	fprintf(stderr, " real %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	elapsed = getCpuTime(1);
	fprintf(stderr, " user %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	elapsed = getCpuTime(2);
	fprintf(stderr, " sys  %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);

}
