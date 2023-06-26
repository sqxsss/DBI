#ifndef SORTEDFILE_H
#define SORTEDFILE_H
#include "DBFile.h"
#include <queue>
#include "BigQ.h"

class SortedFile :public FileGenerator{
    friend class DBFile;

public:
    SortedFile ();
    ~SortedFile();
    int Create (char *fpath, fType f_type, void *startup);
    int Open (char *fpath);
    int Close ();
    void Load (Schema &myschema, char *loadpath);
    void MoveFirst ();
    void Add (Record &addme);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    
private:
    File file;
    Page diskBlock;
    off_t pointer;
    bool writing;
    
    OrderMaker* sortedOrder = nullptr;
    int length;

    char* outpath = nullptr;
    int sumBounder = 0;
    int lower;
    int upper;
    Pipe* inputPipe = new Pipe(100);
    Pipe* outputPipe = new Pipe(100);
    pthread_t* pthread = nullptr;

    void writer();
    void reader();
    static void *consumer (void *arg);
    int Merging (Record *left, Record *literal, Comparison *c);
};


#endif