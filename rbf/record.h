//
// Created by LeoShy on 2024/2/28.
//
#ifndef CSEN380_RECORD_H
#define CSEN380_RECORD_H

#include <vector>
#include <climits>
#include <cmath>
#include <cstring>
#include <iostream>
#include "config.h"


class Record{
    friend class RecordBasedFileManager;
    friend class RecordPage;
public:
    Record(){
    }

    explicit Record(const void *data, const std::vector<Attribute> &rd) {
        SerializeData(data, rd);
        allocated_ = true;
    };

    ~Record(){
        if(allocated_)
        delete[] record_;
    };

    inline auto GetRecord() -> byte* {
        return record_;
    }

    inline auto GetRecordLength() const -> const uint16_t{
        uint16_t field_size  = GetFieldSize();
        uint16_t last_field_size = GetFieldOffset(field_size - 1);
        return last_field_size + field_size * SIZE_FIELD_OFFSET + SIZE_FIELD_SIZE;

    }

    auto GetFieldSize() const -> uint16_t{
        return *reinterpret_cast<uint16_t *>(record_);
    };

    auto GetDataOffset() const -> uint16_t{
        return SIZE_FIELD_SIZE + GetFieldSize() * SIZE_FIELD_OFFSET;
    }

    /* actually it returns where the field ends */
    auto GetFieldOffset(uint16_t field_num) const ->uint16_t{
        if (field_num >= GetFieldSize()){
            return INVALID_OFFSET;
        }
        return *reinterpret_cast<uint16_t *>(record_ + SIZE_FIELD_SIZE + SIZE_FIELD_OFFSET * field_num);
    };

    /* it returns where specific field starts, or we say it 'attribute'(it's just too many duplicate terms meaning
     * the same things, like record/tuple, field/attribute, page/block, which sometimes could be confusing. */
    auto GetAttributeOffset(uint16_t field_num) const ->uint16_t{
        if (field_num >= GetFieldSize()){
            return INVALID_OFFSET;
        }

        if (field_num == 0) return 0;

        return *reinterpret_cast<uint16_t *>(record_ + SIZE_FIELD_OFFSET + SIZE_FIELD_OFFSET * (field_num - 1));
    }

    auto ReadAttribute(Attribute& attribute, uint16_t field_num, void* dst, size_t *length) const -> bool;

    void SerializeData(const void* data, const std::vector<Attribute>& rd);

    void DeserializeData(void* data, const std::vector<Attribute>& rd);

protected:
    inline void SetFieldSize(uint16_t field_size){
        memcpy(record_, &field_size, SIZE_FIELD_SIZE);
    };

    inline void SetFieldOffset(uint16_t field_num, uint16_t new_offset){
        /* don't check validity for field_num */
        memcpy(record_ + SIZE_FIELD_SIZE + SIZE_FIELD_OFFSET * field_num, &new_offset, SIZE_FIELD_OFFSET);
    };
private:
    void SerializeHelper(const std::vector<Attribute> &record_descriptor, const void *data,
                         size_t null_indicator_size);

    void DeserializeHelper(const std::vector<Attribute> &rd, void* data);

    //size_t static constexpr OFFSET_FILED_OFFSET = 2;
    uint16_t static constexpr SIZE_FIELD_SIZE = 2;
    uint16_t static constexpr SIZE_FIELD_OFFSET = 2;
    uint16_t static constexpr INVALID_OFFSET = -1;

    bool allocated_ = false;
    byte* record_;
    size_t record_capacity_{};

    //size_t field_size_{};
};

class Tombstone{
public:
    Tombstone(){
        tombstone_ = new char[TOMBSTONE_SIZE];
    }

    ~Tombstone(){
        delete[] tombstone_;
    }

    void Initialize(const char* data){
        memcpy(tombstone_, data, TOMBSTONE_SIZE);
    }

    void Initialize(const RID& rid){
        SetActualPageID(rid.pageNum);
        SetActualSlotID(rid.slotNum);
    }

    auto GetTombstone() const -> char*{
        return tombstone_;
    }

    auto GetActualPageID() const -> page_id_t {
        return *reinterpret_cast<page_id_t*>(tombstone_);
    }

    auto GetActualSlotID() const -> slot_id_t {
        return *reinterpret_cast<slot_id_t*>(tombstone_ + sizeof(page_id_t));
    }

    void SetActualPageID(page_id_t page_id){
        memcpy(tombstone_, &page_id, sizeof(page_id));
    }

    void SetActualSlotID(slot_id_t slot_id){
        memcpy(tombstone_ + sizeof(page_id_t), &slot_id, sizeof(page_id_t));
    }
protected:
    char * tombstone_;
};

#endif //CSEN380_RECORD_H
