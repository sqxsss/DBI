#include <iostream>

#include "ParseTree.h"
#include "NodeOptimizer.h"

using namespace std;

char* catalog_path = "catalog";
char* dbfile_dir = "";
char* tpch_dir = "";

extern "C" {
  int yyparse(void);   // defined in y.tab.c
}

int main (int argc, char* argv[]) {
  if (yyparse() != 0) {
    exit (1);
  }
  Statistics stats;
  char *statsFile = "Statistics.txt";
  stats.loadRelations();
  stats.Write(statsFile);
  Optimizer optimizer(&stats);
  return 0;
}