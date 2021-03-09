
#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif

#include "db.h"
#include "db_object.h"
#include "db_redblack.h"
#include "db_arena.h"
#include "db_map.h"
#include "db_api.h"

//	if this is a new map file, copy param
//	structure to a new ArenaDef in parent
//	otherwise, return existing arenaDef
//	from the parent.

RedBlack *procParam(DbMap *parent, char *name, int nameLen, Params *params) {
PathStk pathStk[1];
ArenaDef *arenaDef;
RedBlack *rbEntry;
DbAddr *slot;

	//	see if ArenaDef already exists as a child in the parent

	while (true) {
	  lockLatch (parent->arenaDef->nameTree->latch);

	  if ((rbEntry = rbFind(parent->db, parent->arenaDef->nameTree, name, nameLen, pathStk))) {
		arenaDef = (ArenaDef *)(rbEntry + 1);

		if (*arenaDef->dead & KILL_BIT) {
		  unlockLatch (parent->arenaDef->nameTree->latch);
		  yield ();
		  continue;
		}

		unlockLatch (parent->arenaDef->nameTree->latch);
		return rbEntry;
	  }

	  break;
	}

	// create new rbEntry in parent
	// with an arenaDef payload

	if ((rbEntry = rbNew(parent->db, name, nameLen, sizeof(ArenaDef)))) {
		arenaDef = (ArenaDef *)(rbEntry + 1);
	} else {
		unlockLatch(parent->arenaDef->nameTree->latch);
		return NULL;
	}

	//	fill in new arenaDef r/b entry

	arenaDef->creation = db_getEpoch();

	memcpy (arenaDef->params, params, sizeof(arenaDef->params));

	//	allocate slot in parent's openMap array

	arenaDef->id = arrayAlloc(parent->db, parent->arenaDef->childList, sizeof(DbAddr));
	slot = arrayEntry(parent->db, parent->arenaDef->childList, (uint16_t)arenaDef->id);
	slot->bits = rbEntry->addr.bits;

	//	add arenaDef to parent's child arenaDef by name tree

	rbAdd(parent->db, parent->arenaDef->nameTree, rbEntry, pathStk);
	unlockLatch(parent->arenaDef->nameTree->latch);
	return rbEntry;
}

//  get millisecond precision system timestamp epoch

int64_t db_getEpoch(void) {
#ifdef __MACH__ // OS X does not have clock_gettime, use clock_get_time
  clock_serv_t cclock[1];
  mach_timespec_t mts[1];
  host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, cclock);
  clock_get_time(*cclock, mts);
  mach_port_deallocate(mach_task_self(), *cclock);
  return mts->tv_sec * 1000ULL + mts->tv_nsec;
#elif !defined(_WIN32)
  struct timespec ts[1];
  clock_gettime(_XOPEN_REALTIME, ts);
  return ts->tv_sec * 1000ULL + ts->tv_nsec / 1000000ULL;
#else
   int64_t wintime[1];
   GetSystemTimeAsFileTime((FILETIME*)wintime);
   *wintime /= 10000ULL;
   *wintime -= 11644473600000i64;  //1jan1601 to 1jan1970
   return *wintime;
#endif
}

