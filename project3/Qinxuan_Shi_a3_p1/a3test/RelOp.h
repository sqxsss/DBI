#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

typedef struct {
    DBFile &file;
    Pipe &pipe;
    CNF &cnf;
    Record &record;
} SelectFileEntity;

class SelectFile : public RelationalOp { 
	private:
	pthread_t sfthread;

	public:
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

typedef struct {
    Pipe &inputPipe;
    Pipe &outputPipe;
    CNF &cnf;
    Record &record;
} SelectPipeEntity;

class SelectPipe : public RelationalOp {
    private:
    pthread_t spthread;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

typedef struct {
	Pipe *inPipe;
	Pipe *outPipe;
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
} ProjectEntity;

class Project : public RelationalOp { 
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	private:
	pthread_t pthread;
};

typedef struct {
	Pipe *leftInputPipe;
	Pipe *rightInputPipe;
	Pipe *outputPipe;
	CNF *cnf;
	Record *record;
	int length;
} JoinEntity;

class Join : public RelationalOp { 
	private:
	pthread_t jthread;
	int rlength;
	
	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

typedef struct {
	Pipe &inputPipe;
	Pipe &outputPipe;
	OrderMaker *sortedOrder;
	int length;
} DuplicateRemovalEntity;

class DuplicateRemoval : public RelationalOp {
	private:
	pthread_t workerThread;
	int rlength;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

typedef struct {
    Pipe &inputPipe;
    Pipe &outputPipe;
    Function &computeMe;
} SumEntity;

class Sum : public RelationalOp {
    private:
    pthread_t sthread;
	
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

typedef struct {
    Pipe &inputPipe;
    Pipe &outputPipe;
    OrderMaker &AttributeGB;
    Function &computeMe;
    int pages;
} GroupByEntity;

class GroupBy : public RelationalOp {
    private:
    pthread_t gbthread;

	public:
    int use_n_pages = 32;
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

typedef struct {
	Pipe &inputPipe;
	FILE *file;
	Schema &mySchema;
} WriteOutEntity;

class WriteOut : public RelationalOp {
	private:
	pthread_t wothread;

	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
#endif