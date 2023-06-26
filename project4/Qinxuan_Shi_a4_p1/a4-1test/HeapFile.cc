#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "HeapFile.h"
#include "Defs.h"
#include <iostream>

HeapFile::HeapFile () {
    writing = 0;
    pointer = 0;
    isOpen = 0;
}

int HeapFile::Create (char *f_path, fType f_type, void *startup) {
    if (isOpen == 1) {
        return 0;
    }
    file.Open(0, const_cast<char *>(f_path));
    pointer = 0;
    writing = 0;
    isOpen = 1;
    MoveFirst();
    return 1;
}

void HeapFile::Load (Schema &f_schema, char *loadpath) {
    if (isOpen == 0) {
        return;
    }

    FILE * f;
    f = fopen(loadpath, "r");
    if(f == NULL){
        // cout<<"open wrong" << loadpath[2] << endl;
        // exit(1);
        cerr << "Cannot loading file!";
        return;
    }

    Record r;
    while (r.SuckNextRecord (&f_schema, f) == 1) {
        this->Add(r);
    }
    if (writing == 1){
        file.AddPage(&diskBlock, pointer);
    }
}

int HeapFile::Open (char *f_path) {
    if (isOpen == 1) {
        return 0;
    }
    file.Open(1, const_cast<char *>(f_path));
    pointer = 0;
    writing = 0;   //default read
    isOpen = 1;
    MoveFirst();
    return 1;
}

void HeapFile::MoveFirst () {
    if (isOpen == 0) {
        return;
    }
    if (writing == 1) {
        file.AddPage(&diskBlock, pointer);
        writing = 0;
    }
    pointer = 0;
    diskBlock.EmptyItOut();
    if (file.GetLength() > 0) {
        file.GetPage(&diskBlock, pointer);
    }
}

int HeapFile::Close () {
    if (isOpen == 0) {
        return 0;
    }
    if (writing == 1){
        file.AddPage(&diskBlock, pointer);
    }
    diskBlock.EmptyItOut();
    file.Close();
    isOpen = 0;
    return 1;
}

void HeapFile::Add (Record &rec) {
    if (isOpen == 0) {
        return;
    }
    if (writing == 0) {
        diskBlock.EmptyItOut();
        if (file.GetLength() > 0) {
            //make the pointer to the last index of page, but since pointer will add one in getpage(), so the pointer should be file.length - 2;
            file.GetPage(&diskBlock, file.GetLength() - 2);
            pointer = file.GetLength() - 2;
        }
        writing = 1;
    }
    if(diskBlock.Append(&rec) == 0){// at the end of page
        file.AddPage(&diskBlock, pointer);  //save the current page and add the record to the new page
        pointer++;
        diskBlock.EmptyItOut();
        diskBlock.Append(&rec);
    }    
}

int HeapFile::GetNext (Record &fetchme) {
    if (isOpen == 0) {
        return 0;
    }
    if (writing == 1) {
        MoveFirst();
    }
    if(diskBlock.GetFirst(&fetchme) == 0){//there is no record in the current page
        pointer++;
        if(pointer >= file.GetLength() - 1){//getpage() will add one in the function, if pointer=file.length()-1, out of index
            return 0;
        }
        diskBlock.EmptyItOut();
        file.GetPage(&diskBlock, pointer);
        diskBlock.GetFirst(&fetchme);
    }
    return 1;
}

int HeapFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    // use the unary operation compare in ComparisonEngine.h, if return 1, the record fulfill the cnf
    ComparisonEngine comp;

    while (GetNext(fetchme) == 1) {
        if (comp.Compare(&fetchme, &literal, &cnf) == 1)
            return 1;
    }
    return 0;
}