#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedFile.h"
#include "Defs.h"
#include <chrono>
#include <thread>
#include <pthread.h>
#include <set>
#include <string.h>

SortedFile::SortedFile () {
    writing = false;
    pointer = 0;
}

SortedFile ::~SortedFile () {
    delete inputPipe;
    delete outputPipe;
}

int SortedFile::Create (char *f_path, fType f_type, void *startup) {
    pointer = 0;
    writing = false;
    file.Open(0, const_cast<char *>(f_path));
    outpath = f_path;
    return 1;
}

int SortedFile::Open (char *f_path) {
    file.Open(1, const_cast<char *>(f_path));
    pointer = 0;
    writing = false;
    outpath = f_path;    
    return 1;
}

int SortedFile::Close () {
    reader();
    diskBlock.EmptyItOut();
    file.Close();
    return 1;
}

void SortedFile::Load (Schema &f_schema, char *loadpath) {
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
    fclose(f);
}

void SortedFile::MoveFirst () {
    reader();
    pointer = 0;
    diskBlock.EmptyItOut();
    if (file.GetLength() > 0) {    //If file is not empty
        file.GetPage(&diskBlock, pointer);
    }
}

void SortedFile::Add (Record &rec) {
    sumBounder = 0;
    if(writing==false) {
        writing = true;
        bigQEntity* entity = new bigQEntity;
        entity->sortedOrder = sortedOrder;
        entity->runLength = length;
        entity->input = inputPipe;
        entity->output = outputPipe;
        pthread = new pthread_t();
        pthread_create(pthread, NULL, TPMMS, (void *) entity);
    }
    inputPipe->Insert(&rec);
}

int SortedFile::GetNext (Record &fetchme) {
    reader();
    if (diskBlock.GetFirst(&fetchme) == 0) {
        pointer++;
        if (pointer >= file.GetLength() - 1) {
            return 0;
        }
        diskBlock.EmptyItOut();
        file.GetPage(&diskBlock, pointer);
        diskBlock.GetFirst(&fetchme);
    }
    return 1;
}

int SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    if(!sumBounder) {
        sumBounder = 1;
        set<int> attributes;
        for (int i = 0; i < sortedOrder->numAtts; i++) {
            attributes.insert(sortedOrder->whichAtts[i]);
        }
        int LOW = 0;
        int UP = file.GetLength() - 2;
        for (int i = 0; i < cnf.numAnds; i++) {
            for (int j = 0; j < cnf.orLens[i]; j++) {
                if (attributes.find(cnf.orList[i][j].whichAtt1) == attributes.end()){
                    continue;
                }
                //use Binary Search to calculate the lower bound and upper bound
                if (cnf.orList[i][j].op == LessThan) {
                    int low = 0;
                    int high = file.GetLength() - 2;
                    Record record;
                    while (low < high) {
                        int mid = low + (high - low + 1) / 2;
                        file.GetPage(&diskBlock, mid);
                        diskBlock.GetFirst(&record);
                        int result = Merging(&record, &literal, &cnf.orList[i][j]);
                        if (result != 0) {
                            low = mid;
                        } else {
                            high = mid - 1;
                        }
                    }
                    UP = min(UP, high);
                }else if (cnf.orList[i][j].op == GreaterThan) {
                    int low = 0;
                    int high = file.GetLength() - 2;
                    Record record;
                    while (low < high) {
                        int mid = low + (high - low) / 2;
                        file.GetPage(&diskBlock, mid);
                        diskBlock.GetFirst(&record);
                        int result = Merging(&record, &literal, &cnf.orList[i][j]);
                        if (result != 0) {
                            high = mid;
                        } else {
                            low = mid + 1;
                        }
                    }
                    LOW = max(LOW, low);
                }
            }
        }
        LOW = LOW - 1;
        LOW = max(0, LOW);

        lower = LOW;
        upper = UP;
        pointer = LOW;
    }

    ComparisonEngine engine;
    while(true) {
        int hasRecord = this->GetNext(fetchme);
        if(hasRecord != 1){  // at the end of file and no records any more
            break;
        }else{
            if (pointer > upper){
                return 0;
            }
            if(engine.Compare(&fetchme, &literal, &cnf) == 1){
                return 1;
            }   
        }
    }
    return 0;
}

int SortedFile :: Merging (Record *left, Record *literal, Comparison *c) {
    char *a, *b;

    char *leRecordBits = left->GetBits();
    char *liRecordBits = literal->GetBits();
    if (c->operand1 == Left) {   //first value to compare
        a = leRecordBits + ((int *) leRecordBits)[c->whichAtt1 + 1];
    } else {
        a = liRecordBits + ((int *) liRecordBits)[c->whichAtt1 + 1];
    }
    if (c->operand2 == Left) {   //second value to compare
        b = leRecordBits + ((int *) leRecordBits)[c->whichAtt2 + 1];
    } else {
        b = liRecordBits + ((int *) liRecordBits)[c->whichAtt2 + 1];
    }

    int i1, i2, t;
    double d1, d2;
    if(c->attType==Int){
        i1 = *((int *) a);
        i2 = *((int *) b);
        if(c->op==LessThan) {
            return i1<i2;
        } else if(c->op==GreaterThan){
            return i1>i2;
        } else{
            return i1==i2;
        }
    } else if(c->attType==Double){
        d1 = *((double *) a);
        d2 = *((double *) b);
        if(c->op==LessThan) {
            return d1<d2;
        } else if(c->op==GreaterThan){
            return d1>d2;
        } else{
            return d1==d2;
        }
    }else{
        t = strcmp(a, b);
        if(c->op==LessThan) {
            return t<0;
        } else if(c->op==GreaterThan){
            return t>0;
        } else{
            return t=0;
        }
    }
}


void SortedFile::reader(){
    if(writing){
        sumBounder = 0;
        writing = false;
        inputPipe->ShutDown();
        if(pthread!= nullptr) {
            pthread_join(*pthread, NULL);
            delete pthread;
        }

        char* f_total = "total.bin";
        char* f_diff = "diffTemp.bin";
        DBFile resultFile;
        resultFile.Create(f_total, heap, nullptr);

        DBFile diffTempF;
        diffTempF.Open(f_diff);
        this->MoveFirst();

        Record r1;
        Record r2;
        ComparisonEngine engine;
        int st1 = diffTempF.GetNext(r1);
        int st2 = this->GetNext(r2);
        while(st1 && st2){
            if (engine.Compare(&r1, &r2, sortedOrder) < 0){
                resultFile.Add(r1);
                st1 = diffTempF.GetNext(r1);
            }else{
                resultFile.Add(r2);
                st2 = this->GetNext(r2);
            }
        }
        while(st1){
            resultFile.Add(r1);
            st1 = diffTempF.GetNext(r1);
        }
        while(st2){
            resultFile.Add(r2);
            st2 = this->GetNext(r2);
        }
        diffTempF.Close();
        resultFile.Close();
        file.Close();
        remove(f_diff);
        remove(outpath);
        rename(f_total, outpath);
    }
}