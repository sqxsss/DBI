#include "BigQ.h"
#include "DBFile.h"

//almost the same as a2_p1
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    this->input = &in;
    this->output = &out;
    this->sortedOrder = &sortorder;
    this->runLength = runlen;
    pthread_t worker;
    pthread_create(&worker, NULL, TPMMS, (void*) this);
}

BigQ::~BigQ () {
}

ComparisonR::ComparisonR(OrderMaker* orderMaker) {
    sortedOrder = orderMaker;
}

ComparisonM::ComparisonM(OrderMaker* orderMaker) {
    sortedOrder = orderMaker;
}

bool ComparisonR::operator() (Record* leftR, Record* rightR) {
    ComparisonEngine engine;
    if (engine.Compare(leftR, rightR, sortedOrder) >= 0){
        return true;
    }
    return false;
}

bool ComparisonM::operator () (Merging* leftM, Merging* rightM) {
    ComparisonEngine engine;
    if (engine.Compare(leftM->first, rightM->first, sortedOrder) >= 0){
        return true;
    }
    return false;
}

void* TPMMS(void* arg) {
    ((BigQ*) arg)->qMain();
    return nullptr;
}

void BigQ::qMain() {
    Record current;
    // bigQEntity* entity = (bigQEntity*) qEntity;
    priority_queue<Merging*, vector<Merging*>, ComparisonM> mergeQue(this->sortedOrder);
    priority_queue<Record*, vector<Record*>, ComparisonR> recordQue(this->sortedOrder);

    //Set disk based file for sorting
    File file;
    char* fileName = new char[100];
    sprintf(fileName, "%d.bin", pthread_self());
    file.Open(0, fileName);
    //Buffer page used for disk based file
    Page blockBuffer;
    int index = 0;
    int count = 0;
    while (this->input->Remove(&current) == 1) {
        Record* record = new Record;
        record->Copy(&current);
        if (blockBuffer.Append(&current) == 0) {//Add to new buffer if current is full
            count++;
            blockBuffer.EmptyItOut();
            if (count == this->runLength) {//Add to new merge if current is full
                pQueueRecordtoMerge(mergeQue, recordQue, file, blockBuffer, index);
                recordQue = priority_queue<Record*, vector<Record*>, ComparisonR>(this->sortedOrder);
                count = 0;
            }
            blockBuffer.Append(&current);
        }
        recordQue.push(record);
    }
    if (!recordQue.empty()) {
        pQueueRecordtoMerge(mergeQue, recordQue, file, blockBuffer, index);
        recordQue = priority_queue<Record*, vector<Record*>, ComparisonR>(this->sortedOrder);
    }
    Schema schema ("catalog", "supplier");
    while (!mergeQue.empty()) {
        Merging* merge = mergeQue.top();
        mergeQue.pop();
        // dbFileHeap.Add(*(run->topRecord));
        Record* waitToInsert = new Record();
        waitToInsert->Copy(merge->first);
        // waitToInsert->Print(&schema);
        this->output->Insert(waitToInsert);
        if (merge->updateFirst() == 1) {
            mergeQue.push(merge);
        }
    }
    file.Close();
    this->output->ShutDown();
    remove(fileName);
}

void* BigQ::pQueueRecordtoMerge(priority_queue<Merging*, vector<Merging*>, ComparisonM>& mergeQue, priority_queue<Record*, vector<Record*>, ComparisonR>& recordQue, 
    File& file, Page& blockBuffer, int& index){
    int start = index;
    blockBuffer.EmptyItOut();
    while (!recordQue.empty()) {
        Record* record = new Record;
        record->Copy(recordQue.top());
        recordQue.pop();
        if (blockBuffer.Append(record) == 0) {
            file.AddPage(&blockBuffer, index++);
            blockBuffer.EmptyItOut();
            blockBuffer.Append(record);
        }
    }
    file.AddPage(&blockBuffer, index++);
    blockBuffer.EmptyItOut();
    Merging* merge = new Merging(&file, start, index-start);
    mergeQue.push(merge);
    return nullptr;
}

Merging::Merging(File* fileM, int beginPage, int runLength) {
    begin = beginPage;
    runlen = runLength;
    current = beginPage;
    file = fileM;
    file->GetPage(&blockBuffer, begin);
    first = new Record;
    updateFirst();
}

int Merging::updateFirst() {
    if (blockBuffer.GetFirst(first) == 0) {//if buffer is full
        current++;
        if (current == runlen + begin) {
            return 0;
        }
        blockBuffer.EmptyItOut();
        file->GetPage(&blockBuffer, current);
        blockBuffer.GetFirst(first);
    }
    return 1;
}

