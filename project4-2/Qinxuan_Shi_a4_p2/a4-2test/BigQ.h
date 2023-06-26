#ifndef BIGQ_H
#define BIGQ_H
#pragma once

#include <algorithm>
#include <iostream>
#include <pthread.h>
#include <stdio.h>
#include <string>
#include <queue>
#include <vector>

#include "ComparisonEngine.h"
#include "DBFile.h"
#include "File.h"
#include "Pipe.h"
#include "Record.h"

class DBFile; 

using namespace std;

class BigQ {
	private:
		DBFile * dbFile;
		File runFile;
		int runLength;
		OrderMaker &orderMarker;
		Pipe &inPipe, &outPipe;
		pthread_t bigQthread;
		string tmpFile;

	public:
		BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
		~BigQ ();
		
		static void * bigQMain(void *bigQ);
};

#endif