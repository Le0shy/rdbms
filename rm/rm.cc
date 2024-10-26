#include "rm.h"

// Define the table descriptor
vector<Attribute> tableDescriptor = {
          Attribute{"table-id", TypeInt, (AttrLength)4},
          Attribute{"table-name", TypeVarChar, (AttrLength)50},
          Attribute{"file-name", TypeVarChar, (AttrLength)50}
};

// Define the column descriptor
vector<Attribute> columnDescriptor = {
          Attribute{"table-id", TypeInt, (AttrLength)4},
          Attribute{"column-name", TypeVarChar, (AttrLength)50},
          Attribute{"column-type", TypeInt, (AttrLength)4},
          Attribute{"column-length", TypeInt, (AttrLength)4},
          Attribute{"column-position", TypeInt, (AttrLength)4}
};

RelationManager* RelationManager::instance()
{
    static RelationManager _rm;
    _rm.rbfm_ = RecordBasedFileManager::instance();
    return &_rm;
}

RelationManager::RelationManager()
{
}

RelationManager::~RelationManager()
{
}

void RelationManager::prepareTablesRecord(int fieldCount, unsigned char *nullFieldsIndicator, 
    const int tableId, const string &tableName, const string &fileName, void *buffer, int bufferSize)
{
    memset(buffer, 0, bufferSize);

    int offset = 0;

    bool nullBit = false;
    int nullFieldsIndicatorActualSize = ceil((double) fieldCount / CHAR_BIT);

    memcpy((char *)buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    // Check if the tableId is null
    nullBit = nullFieldsIndicator[0] & (1 << 7);
    if (!nullBit) {
        memcpy((char *)buffer + offset, &tableId, sizeof(int));
        offset += sizeof(int);
    }

    // Check if the tableName is null
    nullBit = nullFieldsIndicator[0] & (1 << 6);
    if (!nullBit) {
        int tableNameLength = tableName.length();
        memcpy((char *)buffer + offset, &tableNameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)buffer + offset, tableName.c_str(), tableNameLength);
        offset += tableNameLength;
    }

    // Check if the fileName is null
    nullBit = nullFieldsIndicator[0] & (1 << 5);
    if (!nullBit) {
        int fileNameLength = fileName.length();
        memcpy((char *)buffer + offset, &fileNameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)buffer + offset, fileName.c_str(), fileNameLength);
        offset += fileNameLength;
    }
}

void RelationManager::prepareColumnsRecord(int fieldCount, unsigned char* nullFieldsIndicator,
    const int tableId, const string& columnName, const int columnType,
    const int columnLength, const int columnPosition, void* buffer, int bufferSize)
{
    memset(buffer, 0, bufferSize);

    int offset = 0;

    bool nullBit = false;
    int nullFieldsIndicatorActualSize = ceil((double) fieldCount / CHAR_BIT);

    memcpy((char *)buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    // Check if the tableId is null
    nullBit = nullFieldsIndicator[0] & (1 << 7);
    if (!nullBit) {
        memcpy((char *)buffer + offset, &tableId, sizeof(int));
        offset += sizeof(int);
    }

    // Check if the columnName is null
    nullBit = nullFieldsIndicator[0] & (1 << 6);
    if (!nullBit) {
        int columnNameLength = columnName.length();
        memcpy((char *)buffer + offset, &columnNameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)buffer + offset, columnName.c_str(), columnNameLength);
        offset += columnNameLength;
    }

    // Check if the columnType is null
    nullBit = nullFieldsIndicator[0] & (1 << 5);
    if (!nullBit) {
        memcpy((char *)buffer + offset, &columnType, sizeof(int));
        offset += sizeof(int);
    }

    // Check if the columnLength is null
    nullBit = nullFieldsIndicator[0] & (1 << 4);
    if (!nullBit) {
        memcpy((char *)buffer + offset, &columnLength, sizeof(int));
        offset += sizeof(int);
    }

    // Check if the columnPosition is null
    nullBit = nullFieldsIndicator[0] & (1 << 3);
    if (!nullBit) {
        memcpy((char *)buffer + offset, &columnPosition, sizeof(int));
        offset += sizeof(int);
    }
}

RC RelationManager::createCatalog()
{
    RC rc;

    // Create the system files and check
    rbfm_->createFile("Tables");
    rbfm_->createFile("Columns");

    // Write to the system tables
    FileHandle tableHandle;
    rbfm_->openFile("Tables", tableHandle);

    int nullFieldsIndicatorActualSize = ceil((double) 3 / CHAR_BIT);
    unsigned char* nullIndicator = (unsigned char*)malloc(nullFieldsIndicatorActualSize);
    memset(nullIndicator, 0, nullFieldsIndicatorActualSize);

    RID rid;
    int bufferSize = 100; // Change if necessary
    void* record = malloc(bufferSize);
    prepareTablesRecord(3, nullIndicator, 1, "Tables", "Tables", record, bufferSize);
    rbfm_->insertRecord(tableHandle, tableDescriptor, record, rid);
    prepareTablesRecord(3, nullIndicator, 2, "Columns", "Columns", record, bufferSize);
    rbfm_->insertRecord(tableHandle, tableDescriptor, record, rid);
    free(record);
    free(nullIndicator);

    rbfm_->closeFile(tableHandle);

    // Write to the system columns
    FileHandle columnHandle;
    rbfm_->openFile("Columns", columnHandle);

    nullFieldsIndicatorActualSize = ceil((double) 5 / CHAR_BIT);
    nullIndicator = (unsigned char*)malloc(nullFieldsIndicatorActualSize);
    memset(nullIndicator, 0, nullFieldsIndicatorActualSize);

    record = malloc(bufferSize);
    prepareColumnsRecord(5, nullIndicator, 1, "table-id", TypeInt, 4, 1, record, bufferSize);
    rbfm_->insertRecord(columnHandle, columnDescriptor, record, rid);
    prepareColumnsRecord(5, nullIndicator, 1, "table-name", TypeVarChar, 50, 2, record, bufferSize);
    rbfm_->insertRecord(columnHandle, columnDescriptor, record, rid);
    prepareColumnsRecord(5, nullIndicator, 1, "file-name", TypeVarChar, 50, 3, record, bufferSize);
    rbfm_->insertRecord(columnHandle, columnDescriptor, record, rid);
    prepareColumnsRecord(5, nullIndicator, 2, "table-id", TypeInt, 4, 1, record, bufferSize);
    rbfm_->insertRecord(columnHandle, columnDescriptor, record, rid);
    prepareColumnsRecord(5, nullIndicator, 2, "column-name", TypeVarChar, 50, 2, record, bufferSize);
    rbfm_->insertRecord(columnHandle, columnDescriptor, record, rid);
    prepareColumnsRecord(5, nullIndicator, 2, "column-type", TypeInt, 4, 3, record, bufferSize);
    rbfm_->insertRecord(columnHandle, columnDescriptor, record, rid);
    prepareColumnsRecord(5, nullIndicator, 2, "column-length", TypeInt, 4, 4, record, bufferSize);
    rbfm_->insertRecord(columnHandle, columnDescriptor, record, rid);
    prepareColumnsRecord(5, nullIndicator, 2, "column-position", TypeInt, 4, 5, record, bufferSize);
    rbfm_->insertRecord(columnHandle, columnDescriptor, record, rid);
    free(record);
    free(nullIndicator);

    rbfm_->closeFile(columnHandle);

    return 0;
}

RC RelationManager::deleteCatalog()
{
    RC rc;

    // Destroy the system files
    rc = rbfm_->destroyFile("Tables");
    if (rc != 0) return rc;
    rc = rbfm_->destroyFile("Columns");
    if (rc != 0) return rc;

    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RC rc;
    RID rid;
    int bufferSize = 100; // Change if necessary

    // Create the table file
    rc = rbfm_->createFile(tableName);
    if (rc != 0) return rc;

    // Write to the system tables
    FileHandle tableHandle;
    rbfm_->openFile("Tables", tableHandle);
    
    int numTables = tableHandle.recordCounter;
    numTables++;
    int nullFieldsIndicatorActualSize = ceil((double)3 / CHAR_BIT);
    unsigned char* nullIndicator = (unsigned char*)malloc(nullFieldsIndicatorActualSize);
    memset(nullIndicator, 0, nullFieldsIndicatorActualSize);
    void* record = malloc(bufferSize);
    prepareTablesRecord(3, nullIndicator, numTables, tableName, tableName, record, bufferSize);
    rbfm_->insertRecord(tableHandle, tableDescriptor, record, rid);
    free(record);
    free(nullIndicator);

    rbfm_->closeFile(tableHandle);

    // Write to the system columns
    FileHandle columnHandle;
    rbfm_->openFile("Columns", columnHandle);
    
    int position = 1;
    for (const auto &attr : attrs) {
        int nullFieldsIndicatorActualSize = ceil((double)5 / CHAR_BIT);
        nullIndicator = (unsigned char*)malloc(nullFieldsIndicatorActualSize);
        memset(nullIndicator, 0, nullFieldsIndicatorActualSize);
        void* record = malloc(bufferSize);
        prepareColumnsRecord(5, nullIndicator, numTables, attr.name, attr.type, attr.length, position, record, bufferSize);
        rbfm_->insertRecord(columnHandle, columnDescriptor, record, rid);
        free(record);
        free(nullIndicator);
        position++;
    }

    rbfm_->closeFile(columnHandle);

    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    /* catalog can not be deleted */
    if(tableName == "Tables" || tableName == "Columns"){
        return 1;
    }

    RC rc;
    RID rid;
    int bufferSize = 100; // Change if necessary

    // Destroy the table file
    rc = rbfm_->destroyFile(tableName);
    if (rc != 0) return rc;

    // Delete from the system tables
    FileHandle tableHandle;
    rbfm_->openFile("Tables", tableHandle);
    
    char* value = const_cast<char *>(tableName.c_str());
    vector<string> attributeNames;
    attributeNames.push_back("table-id");
    RBFM_ScanIterator rbfmScanIterator;
    rbfm_->scan(tableHandle, tableDescriptor, "table-name", EQ_OP, value, attributeNames, rbfmScanIterator);
    void* data = malloc(bufferSize);
    int tableId = -1;
    while (rbfmScanIterator.getNextRecord(rid, data) != RBFM_EOF) {
        int nullFieldsIndicatorActualSize = ceil((double)3 / CHAR_BIT);
        memcpy(&tableId, (char *)data + nullFieldsIndicatorActualSize, sizeof(int));
        break;
    }
    rbfmScanIterator.close();
    free(data);

    if (tableId == -1) return -1;
    rbfm_->deleteRecord(tableHandle, tableDescriptor, rid);
    rbfm_->closeFile(tableHandle);

    // Delete from the system columns
    FileHandle columnHandle;
    rbfm_->openFile("Columns", columnHandle);
    value = reinterpret_cast<char *>(&tableId);
    attributeNames.clear();
    attributeNames.push_back("table-id");
    rbfm_->scan(columnHandle, columnDescriptor, "table-id", EQ_OP, value, attributeNames, rbfmScanIterator);
    data = malloc(bufferSize);
    while (rbfmScanIterator.getNextRecord(rid, data) != RBFM_EOF) {
        rbfm_->deleteRecord(columnHandle, columnDescriptor, rid);
    }
    rbfmScanIterator.close();
    free(data);

    rbfm_->closeFile(columnHandle);

    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    RC rc;
    RID rid;
    int bufferSize = 100; // Change if necessary

    // Get table ID from system tables
    FileHandle tableHandle;
    rbfm_->openFile("Tables", tableHandle);

    vector<string> attributeNames;
    attributeNames.push_back("table-id");
    void* value = (void*)tableName.c_str();
    RBFM_ScanIterator rbfmScanIterator;
    rbfm_->scan(tableHandle, tableDescriptor, "table-name", EQ_OP, value, attributeNames, rbfmScanIterator);

    void* data = malloc(bufferSize);
    int tableId = -1;
    while (rbfmScanIterator.getNextRecord(rid, data) != RBFM_EOF) {
        int nullFieldsIndicatorActualSize = ceil((double)3 / CHAR_BIT);
        memcpy(&tableId, (char *)data + nullFieldsIndicatorActualSize, sizeof(int));
        break;
    }
    free(data);
    rbfmScanIterator.close();
    rbfm_->closeFile(tableHandle);

    if (tableId == -1) return -1;

    // Get attributes from system columns
    FileHandle columnHandle;
    rbfm_->openFile("Columns", columnHandle);
    attributeNames.clear();
    attributeNames.push_back("column-name");
    attributeNames.push_back("column-type");
    attributeNames.push_back("column-length");
    rbfm_->scan(columnHandle, columnDescriptor, "table-id", EQ_OP, &tableId, attributeNames, rbfmScanIterator);

    data = malloc(bufferSize);
    while (rbfmScanIterator.getNextRecord(rid, data) != RBFM_EOF) {
        Attribute attr;
        int nullFieldsIndicatorActualSize = ceil((double)5 / CHAR_BIT);
        int offset = nullFieldsIndicatorActualSize;
        int nameLength;
        memcpy(&nameLength, (char *)data + offset, sizeof(int));
        offset += sizeof(int);
        char* name = (char *)malloc(nameLength + 1);
        memcpy(name, (char *)data + offset, nameLength);
        name[nameLength] = '\0';
        offset += nameLength;
        attr.name = name;
        int type;
        memcpy(&type, (char *)data + offset, sizeof(int));
        attr.type = (AttrType)type;
        offset += sizeof(int);
        int length;
        memcpy(&length, (char *)data + offset, sizeof(int));
        attr.length = length;
        attrs.push_back(attr);
        free(name);
    }
    free(data);
    rbfmScanIterator.close();

    rbfm_->closeFile(columnHandle);
    
    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    RC rc;
    FileHandle fileHandle;

    // Insert the tuple into the table file
    rc = rbfm_->openFile(tableName, fileHandle);
    if (rc != 0) return rc;
    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    rc = rbfm_->insertRecord(fileHandle, attrs, data, rid);
    if (rc != 0) return rc;
    rc = rbfm_->closeFile(fileHandle);
    if (rc != 0) return rc;

    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    RC rc;
    FileHandle fileHandle;

    // Delete the tuple from the table file
    rc = rbfm_->openFile(tableName, fileHandle);
    if (rc != 0) return rc;
    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    rc = rbfm_->deleteRecord(fileHandle, attrs, rid);
    if (rc != 0) return rc;
    rc = rbfm_->closeFile(fileHandle);
    if (rc != 0) return rc;

    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    RC rc;
    FileHandle fileHandle;

    // Update the tuple in the table file
    rc = rbfm_->openFile(tableName, fileHandle);
    if (rc != 0) return rc;
    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    rc = rbfm_->updateRecord(fileHandle, attrs, data, rid);
    if (rc != 0) return rc;
    rc = rbfm_->closeFile(fileHandle);
    if (rc != 0) return rc;

    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{   
    RC rc;
    FileHandle fileHandle;

    // Read the tuple from the table file
    rc = rbfm_->openFile(tableName, fileHandle);
    if (rc != 0) return rc;
    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    rc = rbfm_->readRecord(fileHandle, attrs, rid, data);
    if (rc != 0) return rc;
    rc = rbfm_->closeFile(fileHandle);
    if (rc != 0) return rc;

    return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{   
    // Print the tuple
    rbfm_->printRecord(attrs, data);
    return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{   
    RC rc;
    FileHandle fileHandle;

    // Read attributes from the table
    rc = rbfm_->openFile(tableName, fileHandle);
    if (rc != 0) return rc;
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    rc = rbfm_->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
    if (rc != 0) return rc;
    rc = rbfm_->closeFile(fileHandle);
    if (rc != 0) return rc;

    return 0;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      void *value,
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    rm_ScanIterator.Initialize(rbfm_, tableName, recordDescriptor, conditionAttribute, compOp,
                               value, attributeNames);

    return 0;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    RC rc;
    RID rid;
    int bufferSize = 100; // Change if necessary 

    // Get table ID from system tables
    FileHandle tableHandle;
    rbfm_->openFile("Tables", tableHandle);

    vector<string> attributeNames;
    attributeNames.push_back("table-id");
    void* value = (void*)tableName.c_str();
    RBFM_ScanIterator rbfmScanIterator;
    rbfm_->scan(tableHandle, tableDescriptor, "table-name", EQ_OP, value, attributeNames, rbfmScanIterator);

    void* data = malloc(bufferSize);
    int tableId = -1;
    while (rbfmScanIterator.getNextRecord(rid, data) != RBFM_EOF) {
        int nullFieldsIndicatorActualSize = ceil((double)3 / CHAR_BIT);
        memcpy(&tableId, (char*)data + nullFieldsIndicatorActualSize, sizeof(int));
        break;
    }
    free(data);
    rbfmScanIterator.close();
    rbfm_->closeFile(tableHandle);

    if (tableId == -1) return -1;

    // Update the system columns
    FileHandle columnHandle;
    rbfm_->openFile("Columns", columnHandle);

    attributeNames.clear();
    attributeNames.push_back("column-name");
    rbfm_->scan(columnHandle, columnDescriptor, "table-id", EQ_OP, &tableId, attributeNames, rbfmScanIterator);

    data = malloc(bufferSize);
    while (rbfmScanIterator.getNextRecord(rid, data) != RBFM_EOF) {
        string columnName;
        int length;
        memcpy(&length, (char*)data + 1, sizeof(int));
        columnName.resize(length);
        memcpy(&columnName[0], (char*)data + 1 + sizeof(int), length);

        if (columnName == attributeName) {
            rbfm_->deleteRecord(columnHandle, columnDescriptor, rid);
            break;
        }
    }
    free(data);
    rbfmScanIterator.close();
    rbfm_->closeFile(columnHandle);

    return 0;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    RC rc;
    RID rid;
    int bufferSize = 100; // Change if necessary 

    vector<Attribute> attrs;
    getAttributes(tableName, attrs);  

    // Get table ID from system tables
    FileHandle tableHandle;
    rbfm_->openFile("Tables", tableHandle);

    vector<string> attributeNames;
    attributeNames.push_back("table-id");
    void* value = (void*)tableName.c_str();
    RBFM_ScanIterator rbfmScanIterator;
    rbfm_->scan(tableHandle, tableDescriptor, "table-name", EQ_OP, value, attributeNames, rbfmScanIterator);

    void* data = malloc(bufferSize);
    int tableId = -1;
    while (rbfmScanIterator.getNextRecord(rid, data) != RBFM_EOF) {
        int nullFieldsIndicatorActualSize = ceil((double)3 / CHAR_BIT);
        memcpy(&tableId, (char*)data + nullFieldsIndicatorActualSize, sizeof(int));
        break;
    }
    free(data);
    rbfmScanIterator.close();
    rbfm_->closeFile(tableHandle);

    if (tableId == -1) return -1;

    // Write to the system columns
    FileHandle columnHandle;
    rbfm_->openFile("Columns", columnHandle);
    attributeNames.clear();
    attributeNames.push_back("column-position");
    rbfm_->scan(columnHandle, columnDescriptor, "table-id", EQ_OP, &tableId, attributeNames, rbfmScanIterator);

    int maxValue = 0;
    int position;
    data = malloc(bufferSize);
    while (rbfmScanIterator.getNextRecord(rid, data) != RBFM_EOF) {
        rbfm_->readAttribute(columnHandle, columnDescriptor, rid, "column-position", data);
        memcpy(&position, data, sizeof(int));
        maxValue = max(maxValue, position);
    }
    free(data);
    rbfmScanIterator.close();

    // Insert the new attribute
    int nullFieldsIndicatorActualSize = ceil((double)5 / CHAR_BIT);
    unsigned char* nullIndicator = (unsigned char*)malloc(nullFieldsIndicatorActualSize);
    memset(nullIndicator, 0, nullFieldsIndicatorActualSize);
    void* record = malloc(bufferSize);
    prepareColumnsRecord(5, nullIndicator, tableId, attr.name, attr.type, attr.length, maxValue + 1, record, bufferSize);
    rbfm_->insertRecord(columnHandle, columnDescriptor, record, rid);
    free(record);
    free(nullIndicator);
    rbfm_->closeFile(columnHandle);

    // Update the table file
    vector<string> fullAttributeNames;
    for (const Attribute &attr : attrs) {
        fullAttributeNames.push_back(attr.name);
    }

    vector<Attribute> newAttrs;
    getAttributes(tableName, newAttrs);
    
    FileHandle fileHandle;
    rbfm_->openFile(tableName, fileHandle);
    rbfm_->scan(fileHandle, attrs, "", NO_OP, NULL, fullAttributeNames, rbfmScanIterator);

    data = malloc(bufferSize);
    while (rbfmScanIterator.getNextRecord(rid, data) != RBFM_EOF) {
        void* newData = malloc(bufferSize);
        memset(newData, 0, bufferSize);

        int nullFieldsIndicatorActualSize = ceil((double) attrs.size() / CHAR_BIT);
        int newNullFieldsIndicatorActualSize = ceil((double) newAttrs.size() / CHAR_BIT);   

        // If the length of null indicator does not change
        if (nullFieldsIndicatorActualSize == newNullFieldsIndicatorActualSize) {
            int bytePosition = nullFieldsIndicatorActualSize - 1;
            int bitPosition = (newAttrs.size() - 1) % CHAR_BIT + 1;
            memcpy(newData, data, bufferSize);
            ((char*)newData)[bytePosition] |= (1 << (CHAR_BIT - bitPosition));
        } 
        // If the length of null indicator changes
        else {
            int bytePosition = newNullFieldsIndicatorActualSize - 1;
            memcpy(newData, data, nullFieldsIndicatorActualSize);
           ((char*)newData)[bytePosition] |= (1 << (CHAR_BIT - 1));
            memcpy((char*)newData + newNullFieldsIndicatorActualSize, (char*)data + nullFieldsIndicatorActualSize, bufferSize - newNullFieldsIndicatorActualSize);
        }

        rbfm_->updateRecord(fileHandle, newAttrs, newData, rid);
        free(newData);
    }
    free(data);
    rbfmScanIterator.close();
    rbfm_->closeFile(fileHandle);

    return 0;
}