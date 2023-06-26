#ifndef A4_1TEST_HEAPFILE_H
#define A4_1TEST_HEAPFILE_H
#include "DBFile.h"

//HeapFile is almost the same as the DBFile in a2_p1, used to implement heap

class HeapFile : public FileGenerator{
    private:
        File file;
        Page diskBlock;
        off_t pointer;
        int writing;
        int isOpen;

    public:
        HeapFile ();
        int Create (char *fpath, fType f_type, void *startup);
        int Open (char *fpath);
        int Close ();
        void Save ();
        void Load (Schema &myschema, char *loadpath);
        void MoveFirst ();
        void Add (Record &addme);
        int GetNext (Record &fetchme);
        int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};

#endif //A4_1TEST_HEAPFILE_H