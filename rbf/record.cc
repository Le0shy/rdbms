//
// Created by LeoShy on 2024/2/28.
//

#include "record.h"

void Record::SerializeData(const void *data, const std::vector<Attribute> &rd) {
    size_t field_size = rd.size();
    size_t null_indicator_size = ceil((double) field_size / CHAR_BIT);
    size_t max_record_size = 0;
    for (size_t i = 0; i < field_size; i += 1) {
        max_record_size += rd[i].length;
        /* very subtle bug make me miserable!!! (the heap space allocated for
         * record won't be enough, maybe just arbitrarily allocate a constant spare me
         * lot trouble !!! */
        if (rd[i].type == TypeVarChar) {
            max_record_size += 4;
        }
    }
    max_record_size += (field_size + 1) * sizeof(uint16_t);

    record_capacity_ = max_record_size;
    record_ = new byte[max_record_size];
    SetFieldSize(field_size);

    SerializeHelper(rd, data, null_indicator_size);
}

void Record::SerializeHelper(const std::vector<Attribute> &record_descriptor, const void *data,
                             size_t null_indicator_size) {
    /* pointer to null indicator value and actual begin of data */
    const char *null_indicator = reinterpret_cast<const char *> (data);
    const char *data_begin = reinterpret_cast<const char *>(data) + null_indicator_size;

    uint32_t field_offset = 0;

    for (size_t i = 0; i < record_descriptor.size(); i += 1) {
        bool is_null = null_indicator[i / 8] & (1 << (7 - i % 8));
        if (is_null) {
            SetFieldOffset(i, field_offset);
        } else {
            switch (record_descriptor[i].type) {
                case TypeInt: {
                    memcpy(GetRecord() + GetDataOffset() + field_offset,
                           data_begin + field_offset, sizeof(int));
                    field_offset += sizeof(int);
                    SetFieldOffset(i, field_offset);
                    break;
                }
                case TypeReal: {
                    memcpy(GetRecord() + GetDataOffset() + field_offset,
                           data_begin + field_offset, sizeof(float));
                    field_offset += sizeof(float);
                    SetFieldOffset(i, field_offset);
                    break;
                }
                case TypeVarChar: {
                    uint32_t length{};
                    memcpy(&length, data_begin + field_offset, sizeof(uint32_t));
                    length += sizeof(uint32_t);

                    memcpy(GetRecord() + GetDataOffset() + field_offset,
                           data_begin + field_offset, length);

                    field_offset += length;
                    SetFieldOffset(i, field_offset);
                    break;
                }
            }
        }
    }
}

void Record::DeserializeData(void *data, const std::vector<Attribute> &rd) {
    unsigned null_indicator_length = ceil((double) rd.size() / CHAR_BIT);
    char *null_indicator = new char[null_indicator_length];
    memset(null_indicator, 0, null_indicator_length);
    // Use `data_field_cur` to store pointer pointing to where it tries to write to in `data`
    char *data_begin = reinterpret_cast<char *> (data) + null_indicator_length;

    uint16_t cur_start{};
    uint16_t cur_end{};
    // Convert field data in record to `data` format
    for (size_t i = 0; i < rd.size(); i += 1) {
        cur_start = cur_end;
        cur_end = GetFieldOffset(i);
        // If the field is null, set nullbit = 1
        if (cur_start == cur_end) {
            null_indicator[i / CHAR_BIT] |= 1 << (7 - i % 8);
        }
            // Copy data from record to `data`
        else {
            switch (rd[i].type) {
                case TypeInt: {
                    memcpy(data_begin + cur_start, GetRecord() + GetDataOffset() + cur_start,
                           sizeof(int));
                    break;
                }
                case TypeReal: {
                    memcpy(data_begin + cur_start, GetRecord() + GetDataOffset() + cur_start,
                           sizeof(float));
                    break;
                }
                case TypeVarChar: {
                    memcpy(data_begin + cur_start, GetRecord() + GetDataOffset() + cur_start,
                           cur_end - cur_start);
                    break;
                }
            }
        }

    }
    memcpy(data, null_indicator, null_indicator_length);
    delete[] null_indicator;
}

auto Record::ReadAttribute(Attribute& attribute, uint16_t field_num, void *dst, size_t *length) const -> bool {
    uint16_t attribute_offset = GetDataOffset() + GetAttributeOffset(field_num);
    uint16_t attribute_length = GetFieldOffset(field_num) - GetAttributeOffset(field_num);

    if (attribute_offset == INVALID_OFFSET) {
        std::cerr << "Invalid offset when read attribute in record" << std::endl;
        return false;
    }

    if (attribute_length == 0) {
        return true;
    }

    *length = attribute_length;

    memcpy(dst, record_ + attribute_offset, attribute_length);

    return true;
}