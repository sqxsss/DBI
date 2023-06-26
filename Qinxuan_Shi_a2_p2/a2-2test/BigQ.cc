#include "BigQ.h"
#include "DBFile.h"

//almost the same as a2_p1
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    bigQEntity* entity = new bigQEntity;
    entity->input = &in;
    entity->output = &out;
    entity->sortedOrder = &sortorder;
    entity->runLength = runlen;
    pthread_t worker;
    pthread_create(&worker, NULL, TPMMS, (void*) entity);
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

void* TPMMS(void* qEntity) {
    Record current;
    bigQEntity* entity = (bigQEntity*) qEntity;
    priority_queue<Merging*, vector<Merging*>, ComparisonM> mergeQue(entity->sortedOrder);
    priority_queue<Record*, vector<Record*>, ComparisonR> recordQue(entity->sortedOrder);

    File file;
    char* fileName = "sortHelp.bin";
    file.Open(0, fileName);

    Page blockBuffer;
    int index = 0;
    int count = 0;
    while (entity->input->Remove(&current) == 1) {
        Record* record = new Record;
        record->Copy(&current);
        if (blockBuffer.Append(&current) == 0) {//Add to new buffer if current is full
            count++;
            blockBuffer.EmptyItOut();
            if (count == entity->runLength) {//Add to new merge if current is full
                pQueueRecordtoMerge(mergeQue, recordQue, file, blockBuffer, index);
                recordQue = priority_queue<Record*, vector<Record*>, ComparisonR>(entity->sortedOrder);
                count = 0;
            }
            blockBuffer.Append(&current);
        }
        recordQue.push(record);
    }
    if (!recordQue.empty()) {
        pQueueRecordtoMerge(mergeQue, recordQue, file, blockBuffer, index);
        recordQue = priority_queue<Record*, vector<Record*>, ComparisonR>(entity->sortedOrder);
    }
    //merge all
    DBFile heapF;
    heapF.Create("diffTemp.bin", heap, nullptr);
    while (!mergeQue.empty()) {
        Merging* merge = mergeQue.top();
        mergeQue.pop();
        heapF.Add(*(merge->first));
        if (merge->updateFirst() == 1) {
            mergeQue.push(merge);
        }
    }
    heapF.Close();
    file.Close();
    entity->output->ShutDown();
    return nullptr;
}

void* pQueueRecordtoMerge(priority_queue<Merging*, vector<Merging*>, ComparisonM>& mergeQue, priority_queue<Record*, vector<Record*>, ComparisonR>& recordQue, 
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