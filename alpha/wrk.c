#include "base64.h"
#include "db.h"
#include "btree1.h"

//	debug slot function


Btree1Slot *btree1Slot(Btree1Page *page, uint32_t idx)
{
	return slotptr(page, idx);
}
