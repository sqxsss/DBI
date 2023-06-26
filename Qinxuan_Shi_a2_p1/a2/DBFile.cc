#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>
// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {

}
int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    if(f_type == heap) { //0 to create file
        file.Open(0, const_cast<char *>(f_path)); //initial all the attributes.
        pointer = 0;
        diskBlock.EmptyItOut();
        writing = false;
    }
    return 1;
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
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
    while(r.SuckNextRecord(&f_schema, f) == 1){
        // cout << "getRecord" << endl;
        // r.Print(&f_schema);
        Add(r);
    }
    Save();
}

void DBFile::Save() {//save diskblock content to file, delete the content in the disblock.
    if(writing) {
        file.AddPage(&diskBlock, pointer);
    }
    writing = false;
    diskBlock.EmptyItOut();
}

int DBFile::Open (const char *f_path) {
    file.Open(1, const_cast<char *>(f_path));   //not 0 is to open the file read and write

    // cout << file.GetLength() << "hahha" << endl;

    pointer = 0;
    writing = false;  //default read
    return 1;
}

void DBFile::MoveFirst () {
    Save();
    pointer = 0;
    file.GetPage(&diskBlock, pointer);
}

int DBFile::Close () {
    Save();   // first save pages that are written
    int length = file.Close();//file.close()return the length of file.
    // cout << length << endl;
    return 1;
}

void DBFile::Add (Record &rec) {
    if(writing){//if it's in writing 
        // cout<< "start get initial data" << endl; 
        // cout<<diskBlock.Append(&rec) << endl;
        if(diskBlock.Append(&rec) == 0){// at the end of page
            file.AddPage(&diskBlock, pointer);  //save the current page and add the record to the new page
            pointer++;
            diskBlock.EmptyItOut();
            diskBlock.Append(&rec);
        }
    }else{// if it is in reading process
        writing = true;
        diskBlock.EmptyItOut();
        pointer = file.GetLength() > 0 ? file.GetLength() -2 : 0; //make the pointer to the last index of page, but since pointer will add one in getpage(), so the pointer should be file.length - 2;
        file.GetPage(&diskBlock, pointer);

        if(diskBlock.Append(&rec) == 0){// at the end of page
            file.AddPage(&diskBlock, pointer);  //save the current page and add the record to the new page
            pointer++;
            diskBlock.EmptyItOut();
            diskBlock.Append(&rec);
        }    
    }
}

int DBFile::GetNext (Record &fetchme) {
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

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {// if the next record fulfill the cnf, return 1 and in test3, it will be print out.
    
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
