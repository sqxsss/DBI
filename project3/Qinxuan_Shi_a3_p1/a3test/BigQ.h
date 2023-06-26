#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <queue>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class Merging {// class used for merging
private: 
	File* file;
	Page blockBuffer;
	int begin;
	int runlen;
	int current;

public:
	Merging(File* fileM, int beginPage, int runLength);
	Record* first;
	int updateFirst();
};

class ComparisonR {//compare records by CNF
private:
	OrderMaker* sortedOrder;

public:
	ComparisonR(OrderMaker *sortedOrder);
	bool operator() (Record* leftR, Record* rightR);
};

class ComparisonM {//compare merging by CNF
private:
	OrderMaker *sortedOrder;

public:
	ComparisonM(OrderMaker *order);
	bool operator() (Merging* leftM, Merging* rightM);
};

typedef struct {
	Pipe *input;
	Pipe *output;
	int runLength;
	OrderMaker *sortedOrder;
} bigQEntity;

//implement the TPMMS algorithm
void* TPMMS(void* qEntity);

class BigQ {
public:

	BigQ (Pipe &input, Pipe &output, OrderMaker &sortedOrder, int runLength);
	~BigQ ();
	void qMain();
	//Use priorith queue to construct class merging for records
	void* pQueueRecordtoMerge(priority_queue<Merging*, vector<Merging*>, ComparisonM>& mergeQue, priority_queue<Record*, vector<Record*>, ComparisonR>& recordQue, 
    File& file, Page& blockBuffer, int& index);

private:
	Pipe *input;
	Pipe *output;
	int runLength;
	OrderMaker *sortedOrder;
};

#endif