//
// Created by LeoShy on 2024/2/27.
//
#include "page.h"

auto RecordPage::InsertRecord(Record &record, RID *rid) -> bool {
    if (GetFreeSpaceRemaining() < record.GetRecordLength() + SIZE_SLOT) {
        return false;
    }

    uint16_t i;
    for (i = 0; i < GetSlotCount(); i++) {
        // If the slot is empty, i.e. its tuple has size 0,
        if (GetRecordSize(i) == 0) {
            // Then we break out of the loop at index i.
            break;
        }
    }

    if (i == GetSlotCount()) {
        SetSlotCount(i + 1);
    }

    // Set the slot.
    SetRecordOffsetAtSlot(i, GetFreeSpacePointer());
    SetRecordSize(i, record.GetRecordLength());

    // claim available free space.
    memcpy(GetPage() + GetFreeSpacePointer(), record.GetRecord(), record.GetRecordLength());
    SetFreeSpacePointer(GetFreeSpacePointer() + record.GetRecordLength());

    rid->pageNum = GetPageId();
    rid->slotNum = i;
    // page is set to dirty
    SetDirty();

    return true;
}

auto RecordPage::DeleteRecord(const RID &rid) -> bool {
    size_t record_size = GetRecordSize(rid.slotNum);
    if(record_size == TOMBSTONE_INDICATOR){
        record_size = TOMBSTONE_SIZE;
    }

    size_t record_offset = GetRecordOffsetAtSlot(rid.slotNum);
    if (record_size == 0){
        /* it does not exist or has been deleted */
        return false;
    }
    char temp[PAGE_SIZE]{};
    size_t next_record_offset = record_offset + record_size;
    size_t move_block_size = GetFreeSpacePointer() - next_record_offset;
    memcpy(temp, GetPage() + record_offset + record_size, move_block_size);

    for(size_t i= 0; i < GetSlotCount(); i += 1){
        size_t iter_offset = GetRecordOffsetAtSlot(i);
        if(iter_offset > record_offset){
            SetRecordOffsetAtSlot(i, iter_offset - record_size);
        }
    }
    /* offset has to be set as zero */
    SetRecordOffsetAtSlot(rid.slotNum, 0);
    SetRecordSize(rid.slotNum, 0);
    /* update free space pointer */
    SetFreeSpacePointer(GetFreeSpacePointer() - record_size);

    memcpy(GetPage() + record_offset, temp, move_block_size);
    return true;
}

auto RecordPage::UpdateRecord(const Record &new_record, const RID &rid) -> bool {
    auto free_space_offset = GetFreeSpacePointer();
    auto record_length = new_record.GetRecordLength();

    SetRecordOffsetAtSlot(rid.slotNum, free_space_offset);
    SetRecordSize(rid.slotNum, record_length);
    memcpy(GetPage() + free_space_offset, new_record.record_, record_length);

    SetFreeSpacePointer(free_space_offset + record_length);
    return true;
}

void RecordPage::DeleteHelper(const RID &rid) {

}

auto RecordPage::ReadRecord(const RID &rid, Record *record) -> bool {
    auto slot_num = rid.slotNum;
    // If somehow we have more slots than tuples, return false.
    if (slot_num >= GetSlotCount()) {
        return false;
    }

    auto tuple_size = GetRecordSize(slot_num);
    // If the tuple is deleted, return 0
    if (GetRecordSize(slot_num) == 0){
        return false;
    }

    auto record_offset = GetRecordOffsetAtSlot(slot_num);
    if (record->allocated_) {
        delete[] record->record_;
    }

    auto record_length = GetRecordSize(slot_num);
    record->record_capacity_ = record_length;
    record->record_ = new char[record_length];
    memcpy(record->GetRecord(), GetPage() + record_offset, record_length);
    record-> allocated_ = true;
    return true;
}


void RecordPage::ReadTombstone(const RID&rid, Tombstone* tombstone){
    if(!IsTombStone(rid)) throw("not a tombstone!\n");

    auto tombstone_offset = GetRecordOffsetAtSlot(rid.slotNum);
    tombstone -> Initialize(GetPage() + tombstone_offset);
}

void RecordPage::WriteTombstone(const RID&rid, Tombstone* tombstone){
    auto free_space_offset = GetFreeSpacePointer();
    SetRecordOffsetAtSlot(rid.slotNum, free_space_offset);
    SetRecordSize(rid.slotNum, TOMBSTONE_INDICATOR);
    memcpy(GetPage() + free_space_offset, tombstone->GetTombstone(), TOMBSTONE_SIZE);

    SetFreeSpacePointer(free_space_offset + TOMBSTONE_SIZE);
}

auto RecordPage::ReadFirstRecordRid(slot_id_t *first_rid) -> bool {
    slot_id_t i = 0;
    for(; i < GetSlotCount(); i += 1){
        uint16_t record_size = GetRecordSize(i);
        if (record_size != 0 && record_size != TOMBSTONE_INDICATOR){
            *first_rid = i;
            return true;
        }
    }
    return false;
}

auto RecordPage::ReadNextRecordRid(const slot_id_t &cur_rid, slot_id_t *next_rid) -> bool {
    slot_id_t i = cur_rid;
    for(i += 1; i < GetSlotCount(); i += 1){
       uint16_t record_size = GetRecordSize(i);
       if(record_size !=0 && record_size != TOMBSTONE_INDICATOR) {
           *next_rid = i;
           return true;
       }
    }
    return false;
}
