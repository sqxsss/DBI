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
    writing = false;
    pointer = 0;
}

int HeapFile::Create (char *f_path, fType f_type, void *startup) {
    file.Open(0, const_cast<char *>(f_path));
    pointer = 0;
    writing = 0;
    return 1;
}

void HeapFile::Load (Schema &f_schema, char *loadpath) {
    pointer = 0;
    writing = true;

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
}

int HeapFile::Open (char *f_path) {
    file.Open(1, const_cast<char *>(f_path));   //not 0 is to open the file read and write
    pointer = 0;
    writing = false;  //default read 
    return 1;
}

void HeapFile::Save() {//save diskblock content to file, delete the content in the disblock.
    if(writing) {
        file.AddPage(&diskBlock, pointer);
    }
    writing = false;
    diskBlock.EmptyItOut();
}

void HeapFile::MoveFirst () {
    Save();
    pointer = 0;
    if (file.GetLength() > 0) {    //If DBfile is not empty
        file.GetPage(&diskBlock, pointer);
    }
}

int HeapFile::Close () {
    Save();
    file.Close();
    // cout << "Closing file, length of file is " << file.GetLength() << "Pages" << "\n";
    return 1;
}

void HeapFile::Add (Record &rec) {
    if(!writing){//if it's in writing 
        diskBlock.EmptyItOut();
        if (file.GetLength() > 0) {
            //make the pointer to the last index of page, but since pointer will add one in getpage(), so the pointer should be file.length - 2;
            file.GetPage(&diskBlock, file.GetLength() - 2);
            pointer = file.GetLength() - 2;
        }
        writing = true;
    }
    if(diskBlock.Append(&rec) == 0){// at the end of page
        file.AddPage(&diskBlock, pointer);  //save the current page and add the record to the new page
        pointer++;
        diskBlock.EmptyItOut();
        diskBlock.Append(&rec);
    }    
}

int HeapFile::GetNext (Record &fetchme) {
    if(diskBlock.GetFirst(&fetchme) == 0){//there is no record in the current page
        pointer++;
        diskBlock.EmptyItOut();
        if(pointer >= file.GetLength() - 1){//getpage() will add one in the function, if pointer=file.length()-1, out of index
            return 0;
        }
        file.GetPage(&diskBlock, pointer);
        diskBlock.GetFirst(&fetchme);
    }
    return 1;
}

int HeapFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    // use the unary operation compare in ComparisonEngine.h, if return 1, the record fulfill the cnf
    ComparisonEngine comp;

    while(true) {
        int hasRecord = this->GetNext(fetchme);
        if(hasRecord != 1){  // at the end of file and no records any more
            break;
        }else{
            if(comp.Compare(&fetchme, &literal, &cnf) == 1){
                return 1;
            }   
        }
    }
    return 0;
}