#include "gtest/gtest.h"
#include <iostream>
#include "Record.h"
#include "DBFile.h"
#include "DBFile.cc"
#include "BigQ.h"
#include "BigQ.cc"
#include <stdlib.h>
using namespace std;

class TestMap: public testing::Test {
   public:
      DBFile* dbFile;

      static void SetUpTestCase(){
         // dbFile = new DBFile();
      }

      static void TearDownTestCase(){
         // delete dbFile;
      }

      void SetUp() {
         dbFile = new DBFile();
         cout << "test_f start" << endl;
      }

      void TearDown() {
         cout << "exit from tear down" << endl;
         delete dbFile;
      }
};

TEST_F(TestMap, bigQ_test_updateFirst1) {
   File file;
   file.Open(1, "./region.bin");
   class Merging* merge = new class Merging(&file, 0, 1);
   Page blockBuffer;
   file.GetPage(&blockBuffer, 0);
   Record temp;
   blockBuffer.GetFirst(&temp);
   while (blockBuffer.GetFirst(&temp) == 1) {
      EXPECT_EQ(merge->updateFirst(), 1);
   }
   EXPECT_EQ(merge->updateFirst(), 0);
   file.Close();
}

TEST_F(TestMap, bigQ_test_updateFirst2) {
   File file;
   file.Open(1, "./nation.bin");
   class Merging* merge = new class Merging(&file, 0, 1);
   Page blockBuffer;
   file.GetPage(&blockBuffer, 0);
   Record temp;
   blockBuffer.GetFirst(&temp);
   while (blockBuffer.GetFirst(&temp) == 1) {
      EXPECT_EQ(merge->updateFirst(), 1);
   }
   EXPECT_EQ(merge->updateFirst(), 0);
   file.Close();
}

TEST_F(TestMap, bigQ_test_ComparisonM) {
    File file;
    file.Open(1, "./lineitem.bin");
    Schema* scheme = new Schema("catalog", "lineitem");
    OrderMaker* order = new OrderMaker(scheme);
    priority_queue<class Merging*, vector<class Merging*>, ComparisonM> MergeQue(order);
    ComparisonEngine comparisonEngine;

    class Merging* m1 = new class Merging(&file, 0, 1);
    class Merging* m2 = new class Merging(&file, 1, 1);
    MergeQue.push(m1);
    MergeQue.push(m2);

    Record one, two;
    one.Copy(MergeQue.top()->first);
    MergeQue.pop();
    two.Copy(MergeQue.top()->first);
    MergeQue.pop();
    EXPECT_EQ(comparisonEngine.Compare(&one, &two, order), -1);

    file.Close();
}

TEST_F(TestMap, bigQ__Test_OpenFile_Test) {
   EXPECT_EQ(dbFile->Open("./nation.bin"), 1);
}

TEST_F(TestMap, bigQ__Test_CloseFile_Test) {
   dbFile->Open("./nation.bin");
   EXPECT_EQ(dbFile->Close(), 1);
}

int main(int argc,char*argv[])
{
   testing::InitGoogleTest(&argc,argv);
   return RUN_ALL_TESTS();
}