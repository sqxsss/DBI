#include "RelOp.h"
#include <iostream>
#include "BigQ.h"

// SelectFile functions
void* SelectFileMain(void* sfEntity){
    SelectFileEntity* entity = (SelectFileEntity*) sfEntity;
    Record r;
    while(entity->file.GetNext(r, entity->cnf, entity->record)){
        entity->pipe.Insert(&r);
    }
    entity->pipe.ShutDown();
    return nullptr;
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    SelectFileEntity* sfentity = new SelectFileEntity{inFile, outPipe, selOp, literal};
    pthread_create(&sfthread, NULL, SelectFileMain, (void*) sfentity);
}

void SelectFile::WaitUntilDone () {
	pthread_join (sfthread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {
}

// SelectPipe functions
void* SelectPipeMain(void* spEntity){
    ComparisonEngine comp;
    SelectPipeEntity* entity = (SelectPipeEntity*) spEntity;
    Record r;
    while(entity->inputPipe.Remove(&r)){
        if(comp.Compare(&r, &entity->record, &entity->cnf)){
            entity->outputPipe.Insert(&r);
        }
    }
    entity->outputPipe.ShutDown();
    return nullptr;
}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
    SelectPipeEntity* spentity = new SelectPipeEntity{inPipe, outPipe, selOp, literal};
    pthread_create(&spthread, NULL, SelectPipeMain, (void*) spentity);
}

void SelectPipe::WaitUntilDone () {
	pthread_join (spthread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {
}

// project functions
void* ProjectMain(void* qEntity) {
    ProjectEntity* pentity = (ProjectEntity*) qEntity;
    Record record;
    while (pentity->inPipe->Remove(&record) == 1) {
        Record* tempRecord = new Record;
        tempRecord->Consume(&record);
        tempRecord->Project(pentity->keepMe, pentity->numAttsOutput, pentity->numAttsInput);
        pentity->outPipe->Insert(tempRecord);     
    }
    pentity->outPipe->ShutDown();
    return nullptr;
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
   ProjectEntity* pentity = new ProjectEntity;
    pentity->inPipe = &inPipe;
    pentity->outPipe = &outPipe;
    pentity->keepMe = keepMe;
    pentity->numAttsInput = numAttsInput;
    pentity->numAttsOutput = numAttsOutput;
    pthread_create(&pthread, NULL, ProjectMain, (void*) pentity);
}

void Project::WaitUntilDone() {
    pthread_join(pthread, NULL);
}

void Project::Use_n_Pages(int n) {

}

// DuplicateRemoval functions
void* DuplicateRemovalMain(void* drEntity) {
    DuplicateRemovalEntity* entity = (DuplicateRemovalEntity*) drEntity;
    ComparisonEngine comparisonEngine;
    Record record;
    Pipe* pipe = new Pipe(1000);
    BigQ* bigQ = new BigQ(entity->inputPipe, *pipe, *(entity->sortedOrder), entity->length);
    pipe->Remove(&record);
    Schema schema ("catalog", "partsupp");
    Record r;
    while (pipe->Remove(&r) == 1) {
        if (comparisonEngine.Compare(&record, &r, entity->sortedOrder) != 0) {
            Record* temp = new Record;
            temp->Consume(&record);
            entity->outputPipe.Insert(temp);
            record.Consume(&r);
        }
    }
    entity->outputPipe.Insert(&record);
    entity->outputPipe.ShutDown();
    return nullptr;
}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) { 
    OrderMaker* order = new OrderMaker(&mySchema);
    int len = this->rlength <=0 ? 8:this->rlength;
    DuplicateRemovalEntity* drentity = new DuplicateRemovalEntity{inPipe, outPipe, order, len};
    pthread_create(&workerThread, NULL, DuplicateRemovalMain, (void*) drentity);
}

void DuplicateRemoval::WaitUntilDone () { 
    pthread_join(workerThread, NULL);
}

void DuplicateRemoval::Use_n_Pages (int n) {
    this->rlength = n;
}

// Join functions
void mergeRecord(Record* left, Record* right, Pipe* pipe) { //merge two records
    int lrecAttNum = ((((int*) left->bits)[1]) / sizeof(int)) - 1;
    int rrceAttNum = ((((int*) right->bits)[1]) / sizeof(int)) - 1;
    int* attsToKeep = new int[lrecAttNum + rrceAttNum];
    for (int i = 0; i < lrecAttNum; i++)
        attsToKeep[i] = i;
    for (int i = lrecAttNum; i < lrecAttNum + rrceAttNum; i++)
        attsToKeep[i] = i - lrecAttNum;
    Record rec;
    rec.MergeRecords(left, right, lrecAttNum, rrceAttNum, attsToKeep, lrecAttNum + rrceAttNum, lrecAttNum);
    pipe->Insert(&rec);
}

void sortMergedRecord(JoinEntity* jEntity, OrderMaker* lorder, OrderMaker* rorder) { //sort
    Record left;
    Record right;
    Pipe* leftPipe = new Pipe(1000);
    Pipe* rightPipe = new Pipe(1000);
    BigQ* lbigQ = new BigQ(*(jEntity->leftInputPipe), *leftPipe, *lorder, jEntity->length);
    BigQ* rbigQ = new BigQ(*(jEntity->rightInputPipe), *rightPipe, *rorder, jEntity->length);
    bool toEnd = false;
    if (leftPipe->Remove(&left) == 0)
        toEnd = true;
    if (rightPipe->Remove(&right) == 0)
        toEnd = true;
    ComparisonEngine engine;
    while (!toEnd) {
        int compareRes = engine.Compare(&left, lorder, &right, rorder);
        if (compareRes == 0) {
            vector<Record*> leftRV;
            vector<Record*> rightRV;
            while (true) {
                Record* oldleft = new Record;
                oldleft->Consume(&left);
                leftRV.push_back(oldleft);
                if (leftPipe->Remove(&left) == 0) {
                    toEnd = true;
                    break;
                }
                if (engine.Compare(&left, oldleft, lorder) != 0) {
                    break;
                }
            }
            while (true) {
                Record* oldright = new Record;
                oldright->Consume(&right);
                rightRV.push_back(oldright);
                if (rightPipe->Remove(&right) == 0) {
                    toEnd = true;
                    break;
                }
                if (engine.Compare(&right, oldright, rorder) != 0) {
                    break;
                }
            }
            for (int i = 0; i < leftRV.size(); i++) {
                for (int j = 0; j < rightRV.size(); j++) {
                    mergeRecord(leftRV[i], rightRV[j], jEntity->outputPipe);
                }
            }
            leftRV.clear();
            rightRV.clear();
        }
        else if (compareRes > 0) {
            if (rightPipe->Remove(&right) == 0)
                toEnd = true;
        }
        else {
            if (leftPipe->Remove(&left) == 0)
                toEnd = true;
        }
    }
    while (leftPipe->Remove(&left) == 1);
    while (rightPipe->Remove(&right) == 1);
}

void nestedJoin(JoinEntity* jEntity) {// nested join between blocks
    DBFile file;
    char* name = new char[100];
    sprintf(name, "nestedJoinHelp%d.bin", pthread_self());
    file.Create(name, heap, NULL);
    file.Open(name);
    Record record;
    while (jEntity->leftInputPipe->Remove(&record) == 1){
        file.Add(record);
    }
    Record r1;
    Record r2;
    ComparisonEngine engine;
    while (jEntity->rightInputPipe->Remove(&r1) == 1) {
        file.MoveFirst();
        while (file.GetNext(record) == 1) {
            if (engine.Compare(&r1, &r2, jEntity->record, jEntity->cnf)) {
                mergeRecord(&r1, &r2, jEntity->outputPipe);
            }
        }
    }
}

void* JoinMain(void* jEntity) {
    JoinEntity* entity = (JoinEntity*) jEntity;
    OrderMaker lSortedOrder;
    OrderMaker rSortedOrder;
    entity->cnf->GetSortOrders(lSortedOrder, rSortedOrder);
    if (lSortedOrder.numAtts > 0 && rSortedOrder.numAtts > 0) {
        sortMergedRecord(entity, &lSortedOrder, &rSortedOrder);
    }
    else {
        nestedJoin(entity);
    }
    entity->outputPipe->ShutDown();
    return nullptr;
}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    JoinEntity* jentity = new JoinEntity;
    jentity->leftInputPipe = &inPipeL;
    jentity->rightInputPipe = &inPipeR;
    jentity->outputPipe = &outPipe;
    jentity->cnf = &selOp;
    jentity->record = &literal;
    if (this->rlength <= 0)
        jentity->length = 8;
    else
        jentity->length = this->rlength;
    pthread_create(&jthread, NULL, JoinMain, (void*) jentity);
}

void Join::WaitUntilDone () { 
    pthread_join(jthread, NULL);
}
    
void Join::Use_n_Pages (int n) { 
    this->rlength = n;
}

//Sum functions
void* SumMain(void* sEntity){
    ComparisonEngine engine;
    SumEntity* workerArg = (SumEntity*) sEntity;
    Record rec;
    Type type;
    int sum = 0;
    int result = 0;
    double dsum = 0.0;
    double dresult = 0.0;
    while(workerArg->inputPipe.Remove(&rec)){
        type = workerArg->computeMe.Apply(rec, result, dresult);
        if(type==Int){
            sum += result;
        }
        else{
            dsum += dresult;
        }
    }
    Attribute att = {"SUM", type};
    Schema out_sch ("out_sch", 1, &att);
    Record r;
    char c[100];
    if(type==Int){
        sprintf(c, "%d|", sum);
    }
    else{
        sprintf(c, "%lf|", dsum);
    }
    r.ComposeRecord(&out_sch, c);
    workerArg->outputPipe.Insert(&r);
    workerArg->outputPipe.ShutDown();
    return nullptr;
}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe){
    SumEntity* sentity = new SumEntity{inPipe, outPipe, computeMe};
    pthread_create(&sthread, NULL, SumMain, (void*) sentity);
}
void Sum::WaitUntilDone (){
    pthread_join(sthread, NULL);
}
void Sum::Use_n_Pages (int n){
}

// GroupBy functions
void* GroupByMain(void* gbEntity){
    GroupByEntity* entity = (GroupByEntity*) gbEntity;
    Pipe pipe(100);
    BigQ bigQ(entity->inputPipe, pipe, entity->AttributeGB, entity->pages);
    ComparisonEngine cmp;
    Record pRecord;
    Record cRecord;
    Type type;
    char c[100];
    Attribute att = {"SUM", type};
    Schema out_sch ("out_sch", 1, &att);
    bool firstTime = true;
    int sum = 0;
    int result = 0;
    double dsum = 0.0;
    double dresult = 0.0;
    while(pipe.Remove(&cRecord)){
        if(!firstTime && cmp.Compare(&cRecord, &pRecord, &entity->AttributeGB)!=0){
            Record r;
            char cc[100];
            if(type==Int){
                sprintf(cc, "%d|", sum);
            }
            else {
                sprintf(cc, "%lf|", dsum);
            }
            r.ComposeRecord(&out_sch, cc);
            entity->outputPipe.Insert(&r);
            sum = 0;
            dsum = 0.0;
        }
        firstTime = false;
        type = entity->computeMe.Apply(cRecord, result, dresult);
        if(type == Int){
            sum += result;
        }
        else {
            dsum += dresult;
        }
        pRecord.Copy(&cRecord);
    }
    // for the record group
    Record res;
    if(type==Int){
        sprintf(c, "%d|", sum);
    }
    else {
        sprintf(c, "%lf|", dsum);
    }
    res.ComposeRecord(&out_sch, c);
    entity->outputPipe.Insert(&res);
    entity->outputPipe.ShutDown();
    return nullptr;
}

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe){
    GroupByEntity* gbentity = new GroupByEntity{inPipe, outPipe, groupAtts, computeMe, use_n_pages};
    pthread_create(&gbthread, NULL, GroupByMain, (void*) gbentity);
}

void GroupBy::WaitUntilDone (){
    pthread_join(gbthread, NULL);
}

void GroupBy::Use_n_Pages (int n){
    use_n_pages = n;
}

// WriteOut functions
void* WriteOutMain(void* woEntity) {
    WriteOutEntity* entity = (WriteOutEntity*) woEntity;
    Record cRecord;
    while (entity->inputPipe.Remove(&cRecord) == 1) {
        int attributeNum = entity->mySchema.GetNumAtts();
        Attribute *attribute = entity->mySchema.GetAtts();
        for (int i = 0; i < attributeNum; i++) {
            fprintf(entity->file, "%s:", attribute[i].name);
            int pointer = ((int *) cRecord.bits)[i + 1];
            fprintf(entity->file, "[");
            if (attribute[i].myType == Int) {
                int *woInt = (int*) &(cRecord.bits[pointer]);
                fprintf(entity->file, "%d", *woInt);
            }
            else if (attribute[i].myType == Double) {
                double *woDouble = (double*) &(cRecord.bits[pointer]);
                fprintf(entity->file, "%f", *woDouble);
            }
            else if (attribute[i].myType == String) {
                char* woString = (char*) &(cRecord.bits[pointer]);
                fprintf(entity->file, "%s", woString);
            }
            fprintf(entity->file, "]");
            if (i != attributeNum - 1)
                fprintf(entity->file, ", ");
        }
        fprintf(entity->file, "\n");
    }
    return nullptr;
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    WriteOutEntity* woentity = new WriteOutEntity{inPipe, outFile, mySchema};
    pthread_create(&wothread, NULL, WriteOutMain, (void*) woentity);
}

void WriteOut::WaitUntilDone () {
    pthread_join(wothread, NULL);
}

void WriteOut::Use_n_Pages (int n) { 
}