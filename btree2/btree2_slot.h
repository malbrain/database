//  macroes for slot access

#define slotkey(slot) (slot->keyBase + sizeof(Btree2Slot) + slot->height * sizeof(uint16_t))

#define slotptr(page, off) (off ? (Btree2Slot *)((uint8_t *)page + (off << page->skipBits)) : db_abort(off > 0, "slot specified with zero offset", NULL))
