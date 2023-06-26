#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <queue>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

//Class run represent run used for merging
class Run {
	public:
		Run(File* file, int startPage, int runLength);
		int UpdateTopRecord();
		Record *topRecord; 
	private: 
		File* fileBase;
		Page bufferPage;
		int startPage;
		int runLength;
		int curPage;
};

//Class used for comparing records for given CNF
class RecordComparer {
	public:
		bool operator () (Record* left, Record* right);
		RecordComparer(OrderMaker *order);

	private:
		OrderMaker *order;
};

//Class used for comparing run for given CNF
class RunComparer {
	public:
		bool operator () (Run* left, Run* right);
		RunComparer(OrderMaker *order);

	private:
		OrderMaker *order;
};

typedef struct {
	Pipe *in;
	Pipe *out;
	OrderMaker *order;
	int runlen;
} WorkerArg;

void* TPMMS(void* arg);

class BigQ {

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
	void BigQMain();
	void recordQueueToRun(priority_queue<Record*, vector<Record*>, RecordComparer>& recordQueue, 
    priority_queue<Run*, vector<Run*>, RunComparer>& runQueue, File& file, Page& bufferPage, int& pageIndex);


private:
	Pipe *in;
	Pipe *out;
	OrderMaker *order;
	int runlen;

};
#endif