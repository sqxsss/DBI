#ifndef HEAPFILE_H
#define HEAPFILE_H
#include "DBFile.h"

//HeapFile is almost the same as the DBFile in a2_p1, used to implement heap

class HeapFile : public FileGenerator{
private:
    File file;
	Page diskBlock;
	off_t pointer;
	bool writing;

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


#endif