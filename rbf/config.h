#pragma once

#include <cstdint>
#include <cstddef>
#include <string>
// Record ID
typedef struct
{
  unsigned pageNum;    // page number
  unsigned slotNum;    // slot number in the page
} RID;
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

// Attribute
struct Attribute {
    std::string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0, // no condition// =
    LT_OP,      // <
    LE_OP,      // <=
    GT_OP,      // >
    GE_OP,      // >=
    NE_OP,      // !=
    NO_OP       // no condition
} CompOp;

static constexpr uint32_t INVALID_PAGE_ID = -1;                                           // invalid page id
static constexpr int HEADER_PAGE_ID = 0;                                             // the header page id
static constexpr size_t PAGE_SIZE = 4096;                                        // size of a data page in byte 
static constexpr int BUFFER_POOL_SIZE = 16;                                     // size of buffer pool
static constexpr size_t TOMBSTONE_SIZE = 8;

using byte = char;
using RC = int;
using frame_id_t = uint32_t;    // frame id type
using page_id_t = uint32_t;     // page id type
using slot_id_t = uint32_t;  // slot offset type
