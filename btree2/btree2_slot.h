//  macroes for slot access

#define slotkey(slot) ((uint8_t *)(slot + 1) + slot->height * sizeof(uint16_t))
#define slotptr(page, off) (Btree2Slot *)((uint8_t *)page + (off << page->skipBits)) 

#define keylen(key) ((key[0] & 0x80) ? ((key[0] & 0x7f) << 8 | key[1]) : key[0])
#define keystr(key) ((key[0] & 0x80) ? (key + 2) : (key + 1))
#define keypre(key) ((key[0] & 0x80) ? 2 : 1)


uint16_t btree2SlotSize(Btree2Slot *slot, uint8_t skipBits);
