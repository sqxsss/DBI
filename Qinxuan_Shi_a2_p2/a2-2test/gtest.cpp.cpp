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
        dbfile = new DBFile();
        orderMaker.numAtts = 1;
        orderMaker.whichAtts[0] = 4;
        orderMaker.whichTypes[0] = String;
    }

    void TearDown() override {
        delete dbfile;
    }
    DBFile* dbfile;
    OrderMaker orderMaker;
};

TEST_F(TestMap, CreateTest) {
    struct {OrderMaker *o; int l;} temp = {&orderMaker, 16};
    cout<<"=="<< dbfile->Create("./gtest.bin", sorted, &temp) <<endl;
    dbfile->Close();
}

TEST_F(TestMap, OpenTest) {
    EXPECT_EQ(dbfile->Open("./gtest.bin"), 1);
    dbfile->Close();
}

TEST_F(TestMap, CloseTest) {
    dbfile->Open("./gtest.bin");
    EXPECT_EQ(dbfile->Close(), 1);
}

TEST_F(TestMap, GetNextTest) {
    struct {OrderMaker *o; int l;} startup = {&orderMaker, 16};
    dbfile->Open("./gtest.bin");
    Record record;
    EXPECT_EQ(dbfile->GetNext(record), 0);
    dbfile->Close();
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}