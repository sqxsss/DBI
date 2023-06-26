#include "gtest/gtest.h"
#include <iostream>
#include "Record.h"
#include "DBFile.h"
#include "DBFile.cc"
#include "HeapFile.cc"
#include "HeapFile.h"
#include "SortedFile.cc"
#include "SortedFile.h"
#include "BigQ.h"
#include "BigQ.cc"
#include "RelOp.cc"
#include "RelOp.h"
#include "Function.h"
#include <stdlib.h>
#include "Defs.h"

using namespace std;

class TestMap : public ::testing::Test {
protected:
    TestMap() {
        // You can do set-up work for each test here.
    }

    ~TestMap() override {
        // You can do clean-up work that doesn't throw exceptions here.
    }

    static void SetUpTestCase(){
        // dbFile = new DBFile();
    }

    static void TearDownTestCase(){
        // delete dbFile;
    }

    void SetUp() override {
    }

    void TearDown() override {
    }
};

TEST_F(TestMap, Statistic1_Test) {
    SelectPipe selectPipe;
}

TEST_F(TestMap, Statistic2_Test) {
    SelectFile sf;
    sf.Use_n_Pages(1);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}