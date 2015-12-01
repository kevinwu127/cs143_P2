/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <string>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeNode.h"
#include "BTreeIndex.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

   // BTLeafNode node;
   // string filename = "movie.del";
  // PageFile pf(filename, 'r');
  // node.read(0, pf);

  //filename = "test.del";
  //PageFile test(filename, 'r');
  // RecordId rid;
  // rid.pid = 2;
  // rid.sid = 3;
  // node.insert(1,rid);
  // RecordId rid2;
  // rid2.pid = 5;
  // rid2.sid = 6;
  // node.insert(5, rid2);

  // RecordId rid3;
  // rid3.pid = 7;
  // rid3.sid = 8;
  // node.insert(4, rid3);

  // RecordId rid4;
  // rid4.pid = 7;
  // rid4.sid = 8;
  // BTLeafNode sibling;
  // int siblingKey;
  // node.insertAndSplit(3, rid4, sibling, siblingKey);

  // sibling.dump();
  //node.dump();

  // int num = node.getKeyCount();
  // cout << endl << num << endl;

  //node.dump();

  //set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  /*BTNonLeafNode node;
  node.initializeRoot(1, 5, 2);
  node.insert(6, 3);
  node.insert(8, 4);
  node.insert(9, 5);
  node.dump();
  BTNonLeafNode sibling;
  int midKey;
  node.insertAndSplit(10, 10, sibling, midKey);

  node.insert(13,12);
  node.insert(15,17);

  node.dump();
  sibling.dump();

  cout << midKey << endl;



  int num = node.getKeyCount();
   cout << endl << num << endl;*/

  // BTreeIndex tree;

  // tree.open("test.idx", 'w');

  // RecordId rid;
  // rid.pid = 0;
  // rid.sid = 0;

  // for (int i = 1; i < 12000; i++)
  // {
  //   rid.pid = rid.sid = i;
  //   tree.insert(i, rid);
  // }

  // tree.insert(1, rid);
  // tree.insert(2, rid);

  // tree.insert(11999, rid);

  // tree.dump();

  // IndexCursor cursor;

  // tree.locate(0, cursor);

  // cout << cursor.pid << ", " << cursor.eid << endl;

  // int key;
  // RecordId rid2;
  // // while ( tree.readForward(cursor, key, rid2) != -3 )
  // tree.readForward(cursor, key, rid2);
  // {
  //   cout << "Key: " << key << endl;
  //   cout << "pid: " << rid2.pid << " sid:" << rid2.sid << endl;
  // }

  // tree.close();

  // BTreeIndex other;

  // other.open("test.idx", 'r');
  // other.dump();

  // other.close();

  // RecordFile rf;
  // RecordId rid;
  // RC rc;
  // int key;
  // string value;
  // BTreeIndex index;
  // IndexCursor cursor;

  // rf.open("movie.tbl", 'r');
  // index.open("movie.idx", 'r');

  // index.locate(0, cursor);

  // index.readForward(cursor, key, rid);

  // rf.read(rid, key, value);

  // cout << key << ", " << value << endl;

  return 0;
}



RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning

  RC     rc;
  int    key;     
  string value;
  int    count;
  int    diff;

  BTreeIndex index;
  IndexCursor cursor;
  bool need_index = true;
  bool NE_exists = false;

  vector<SelCond> key_constraints;
  vector<SelCond> value_constraints;

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }

  // Sort WHERE conditions into key and value constraints
  for (unsigned i = 0; i < cond.size(); i++)
  {
    switch(cond[i].attr)
    {
      case 1:
        if (cond[i].comp != SelCond::NE) key_constraints.push_back(cond[i]);
        else NE_exists = true;
        break;
      case 2:
        value_constraints.push_back(cond[i]);
        break;
    }
  }
  // TODO: Do not use index for SelCond::NE if thats the only thing that is in key constraints



  // need_index = FALSE for following conditions:
  if ( (attr == 2 || attr == 3) && key_constraints.empty() )
  {
    need_index = false;
  }

  if (NE_exists)
  {
    if (key_constraints.empty()) need_index = false;
    else
    {
      for (unsigned i = 0; i < cond.size(); i++)
      {
        if (cond[i].attr == 1 && cond[i].comp == SelCond::NE)
        {
          key_constraints.push_back(cond[i]);
        }
      }
    }
  }

  // attempt to open index file
  if (need_index && (rc = index.open(table + ".idx", 'r')) < 0)
  {
    need_index = false;
  }




  //need_index = false;

  // if using index, find the starting location and point IndexCursor at it
  if (need_index)
  {
    if (key_constraints.size() > 0)
    {
      // find the EQ, GT, or GE conditionSE
      for (int i = 0; i < key_constraints.size(); i++)
      {
        if (key_constraints[i].comp == SelCond::EQ ||
            key_constraints[i].comp == SelCond::GE)
        {
          rc = index.locate(atoi(key_constraints[i].value), cursor);
          break;
        }
        else if (key_constraints[i].comp == SelCond::GT)
        {
          rc = index.locate(atoi(key_constraints[i].value) + 1, cursor);
          break;
        }
        else
        {
          rc = index.locate(0, cursor);
        }
      }
    }
    else
    {
      rc = index.locate(0, cursor);
    }

    if (rc != RC_NO_SUCH_RECORD && rc != 0)
    {
      fprintf(stderr, "Error in locating starting position");
      goto exit_select;
    }

    count = 0;


    // Then start reading from that location to the max value if in key constraints
    while (index.readForward(cursor, key, rid) != -3)
    {
      // only read from RecordFile if we need the value constraint
      if ( (attr == 1 && !value_constraints.empty()) || attr == 2 || attr == 3 || (attr == 4 && !value_constraints.empty()))
      {
        // read the tuple
        if ((rc = rf.read(rid, key, value)) < 0) {
          fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
          goto exit_select;
        }
      }

      for (unsigned i = 0; i < key_constraints.size(); i++)
      {
        diff = key - atoi(key_constraints[i].value);

        switch (key_constraints[i].comp)
        {
          case SelCond::EQ:
            if (diff != 0) goto exit_select;
            break;
          case SelCond::NE:
            if (diff == 0) goto next_node;
            break;
          case SelCond::GT:
            if (diff <= 0) goto next_node;
            break;
          case SelCond::LT:
            if (diff >= 0) goto exit_select;
            break;
          case SelCond::GE:
            if (diff < 0) goto next_node;
            break;
          case SelCond::LE:
            if (diff > 0) goto exit_select;
            break;

        }
      }

      for (unsigned i = 0; i < value_constraints.size(); i++)
      {
        diff = strcmp(value.c_str(), value_constraints[i].value);

        switch (value_constraints[i].comp)
        {
          case SelCond::EQ:
            if (diff != 0) goto next_node;
            break;
          case SelCond::NE:
            if (diff == 0) goto next_node;
            break;
          case SelCond::GT:
            if (diff <= 0) goto next_node;
            break;
          case SelCond::LT:
            if (diff >= 0) goto next_node;
            break;
          case SelCond::GE:
            if (diff < 0) goto next_node;
            break;
          case SelCond::LE:
            if (diff > 0) goto next_node;
            break;

        }
      }

      count++;

      switch (attr) {
        case 1:  // SELECT key
          fprintf(stdout, "%d\n", key);
          break;
        case 2:  // SELECT value
          fprintf(stdout, "%s\n", value.c_str());
          break;
        case 3:  // SELECT *
          fprintf(stdout, "%d '%s'\n", key, value.c_str());
          break;
      }

      // next node
      next_node:;
    }
  }

  else
  {
    // scan the table file from the beginning
    rid.pid = rid.sid = 0;

    while (rid < rf.endRid()) {
      // read the tuple
      if ((rc = rf.read(rid, key, value)) < 0) {
        fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
        goto exit_select;
      }

      // check the conditions on the tuple
      for (unsigned i = 0; i < cond.size(); i++) {
        // compute the difference between the tuple value and the condition value
        switch (cond[i].attr) {
        case 1:
  	diff = key - atoi(cond[i].value);
  	break;
        case 2:
  	diff = strcmp(value.c_str(), cond[i].value);
  	break;
        }

        // skip the tuple if any condition is not met
        switch (cond[i].comp) {
        case SelCond::EQ:
  	if (diff != 0) goto next_tuple;
  	break;
        case SelCond::NE:
  	if (diff == 0) goto next_tuple;
  	break;
        case SelCond::GT:
  	if (diff <= 0) goto next_tuple;
  	break;
        case SelCond::LT:
  	if (diff >= 0) goto next_tuple;
  	break;
        case SelCond::GE:
  	if (diff < 0) goto next_tuple;
  	break;
        case SelCond::LE:
  	if (diff > 0) goto next_tuple;
  	break;
        }
      }

      // the condition is met for the tuple. 
      // increase matching tuple counter
      count++;

      // print the tuple 
      switch (attr) {
      case 1:  // SELECT key
        fprintf(stdout, "%d\n", key);
        break;
      case 2:  // SELECT value
        fprintf(stdout, "%s\n", value.c_str());
        break;
      case 3:  // SELECT *
        fprintf(stdout, "%d '%s'\n", key, value.c_str());
        break;
      }

      // move to the next tuple
      next_tuple:
      ++rid;
    }
  }


  

  // close the table file and return
  exit_select:



    // print matching tuple count if "select count(*)"
  if (attr == 4) {
    fprintf(stdout, "%d\n", count);
  }
  rc = 0;

  if (need_index) index.close();
  rf.close();
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  /* your code here */
  RecordFile rf;
  RecordId rid;

  RC     rc;
  int    key;     
  string value;
  ifstream file;
  string line;

  BTreeIndex tree;

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'w')) < 0) 
  {
    fprintf(stderr, "Error opening or creating table %s\n", table.c_str());
    return rc;
  }

  // open the index file if necessary
  if (index && (rc = tree.open(table + ".idx", 'w')) < 0) 
  {
    fprintf(stderr, "Error opening or creating index for table %s\n", table.c_str());
    return rc;
  }

  file.open(loadfile.c_str(), std::ifstream::in);

  while(!file.eof())
  {
    getline(file, line);

    if (line == "")
      break;

    if (rc = parseLoadLine(line, key, value) < 0)
    {
      fprintf(stderr, "Error: could not parse line\n");
      break;
    }

    if (rc = rf.append(key, value, rid) < 0)
    {
      fprintf(stderr, "Error: could not add to table\n");
      break;
    }

    if (index && (rc = tree.insert(key, rid)) < 0) 
    {
      fprintf(stderr, "Error: could not add to index for table\n");
      break;
    }
  }

  

  file.close();
  rf.close();
  tree.close();

  return rc;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}