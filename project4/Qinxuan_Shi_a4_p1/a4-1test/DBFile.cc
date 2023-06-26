#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "HeapFile.h"
#include "SortedFile.h"
#include "Defs.h"
#include <iostream>
#include <fstream>
// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
}

DBFile :: ~DBFile () {
}

int DBFile::Close () {
    int res = generator->Close();
    delete generator;
    return res;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
    generator->Load(f_schema, loadpath);
}

void DBFile::MoveFirst () {
    generator->MoveFirst();
}

void DBFile::Add (Record &rec) {
    generator->Add(rec);
}

int DBFile::GetNext (Record &fetchme) {
    return generator->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    return generator->GetNext(fetchme, cnf, literal);
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
    char f_name[100];
    sprintf (f_name, "%s.meta", f_path);
    ofstream f_meta;
    f_meta.open(f_name);
    OrderMaker* om = nullptr;
    int runlen = 0;
    if(f_type == heap){
        f_meta << "heap" << endl;
        generator = new HeapFile();
    }
    else if(f_type==sorted){
        f_meta << "sorted"<< endl;
        generator = new SortedFile();
    }
    if(startup!= nullptr) {
        orderMakerInfo* info = ((orderMakerInfo*)startup);
        om = info->sortedOrder;
        runlen = info->length;
        f_meta << runlen << endl;
        f_meta << om->numAtts << endl;
        for (int i = 0; i < om->numAtts; i++) {
            f_meta << om->whichAtts[i] << endl;
            if (om->whichTypes[i] == Int)
                f_meta << "Int" << endl;
            else if (om->whichTypes[i] == Double)
                f_meta << "Double" << endl;
            else if(om->whichTypes[i] == String)
                f_meta << "String" << endl;
        }
        if(f_type == heap){
        }
        else if(f_type==sorted){
            ((SortedFile*)generator)->sortedOrder = om;
            ((SortedFile*)generator)->length = runlen;
        }
    }
    f_meta.close();
    int res = generator->Create(f_path, f_type, startup);
    return res;
}


int DBFile::Open (char *f_path) {
    OrderMaker* om = new OrderMaker();
    char f_name[100];
    sprintf (f_name, "%s.meta", f_path);
    ifstream f_meta(f_name);

    string s;
    getline(f_meta, s);
    if(s.compare("heap")==0){
        generator = new HeapFile();
    }
    else if(s.compare("sorted")==0){
        generator = new SortedFile();
        string t;
        getline(f_meta, t);
        int runlen = stoi(t);
        t.clear();
        getline(f_meta, t);
        om->numAtts = stoi(t);
        for(int i=0; i<om->numAtts; i++){
            t.clear();
            getline(f_meta, t);
            om->whichAtts[i] = stoi(t);
            t.clear();
            getline(f_meta, t);
            if(t.compare("Int")==0){
                om->whichTypes[i] = Int;
            }
            else if(t.compare("Double")==0){
                om->whichTypes[i] = Double;
            }
            else if(t.compare("String")==0){
                om->whichTypes[i] = String;
            }
        }
        ((SortedFile*)generator)->sortedOrder = om;
        ((SortedFile*)generator)->length = runlen;
        om->Print();
    }
    f_meta.close();
    int res = generator->Open(f_path);
    return res;
}