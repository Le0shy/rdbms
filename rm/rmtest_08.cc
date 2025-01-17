#include "rm_test_util.h"

RC TEST_RM_8(const string &tableName, vector<RID> &rids, vector<int> &sizes)
{
    // Functions Tested for large tables:
    // 1. getAttributes
    // 2. insert tuple
    cout << endl << "***** In RM Test Case 8 *****" << endl;

    RID rid; 
    void *tuple = malloc(4000);
    int numTuples = 2000;

    // GetAttributes
    vector<Attribute> attrs;
    RC rc = rm->getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
	memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    // Insert 2000 tuples into table
    for(int i = 0; i < numTuples; i++)
    {
        // Test insert Tuple
        int size = 0;
        memset(tuple, 0, 2000);
        prepareLargeTuple(attrs.size(), nullsIndicator, i, tuple, &size);

        rc = rm->insertTuple(tableName, tuple, rid);
        std::cout<<"insert:"<<i<<" rid:"<<rid.pageNum<<", "<<rid.slotNum<<endl;
        assert(rc == success && "RelationManager::insertTuple() should not fail.");

        rids.push_back(rid);
        sizes.push_back(size);        
    }

    free(tuple);
    free(nullsIndicator);

    writeRIDsToDisk(rids);
    writeSizesToDisk(sizes);

    cout << "***** Test Case 8 Finished. The result will be examined. *****" << endl << endl;

    return success;
}

int main()
{
    vector<RID> rids;
    vector<int> sizes;

	// Insert Tuple
    RC rcmain = TEST_RM_8("tbl_employee4", rids, sizes);

    return rcmain;
}
