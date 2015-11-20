/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"
#include <iostream>
#include <stdio.h>
#include <string.h>

using namespace std;

#define INVALID_PID -1
#define TREE_DATA_PID 0
#define ROOT_PID 1
#define INSERT_SPLIT -2
#define LAST_LEAF -3


/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = INVALID_PID;
    treeHeight = 0;
    fill(buffer, buffer + PageFile::PAGE_SIZE, 0);
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
	RC rc;

	if (rc = pf.open(indexname, mode) < 0) 
	{ 
		rootPid = INVALID_PID;
		return rc; 
	}

	if (pf.endPid() == 0)
	{
		rootPid = INVALID_PID;
		treeHeight = 0;
		fill(buffer, buffer + PageFile::PAGE_SIZE, 0);
		return 0;
	}

	if (rc = pf.read(TREE_DATA_PID, buffer) < 0)
	{
		rootPid = INVALID_PID;
		return rc;
	}

	memcpy(&rootPid, buffer, sizeof(PageId));
	memcpy(&treeHeight, buffer + sizeof(PageId), sizeof(int));

	if (rootPid < 1 || treeHeight < 0)
	{
		rootPid = INVALID_PID;
		treeHeight = 0;
		fill(buffer, buffer + PageFile::PAGE_SIZE, 0);
	}


    return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
	RC rc;

    if ( rootPid != INVALID_PID && treeHeight >= 0 )
    {
    	memcpy(buffer, &rootPid, sizeof(PageId));
    	memcpy(buffer + sizeof(PageId), &treeHeight, sizeof(int));
    }

    if (rc = pf.write(TREE_DATA_PID, buffer) < 0) { return rc; }

    return pf.close();
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{

	if (treeHeight == 0)
	{
		BTLeafNode first_leaf;
		first_leaf.insert(key, rid);
		rootPid = ROOT_PID;
		treeHeight++;

		return first_leaf.write(rootPid, pf);
	}

	int split_key;
	PageId split_pid;

	return insert_R(key, rid, 1, rootPid, split_key, split_pid);
}

RC BTreeIndex::insert_R(int key, const RecordId& rid, int current_height, PageId current_pid, int& split_key, PageId& split_pid)
{
	RC rc;

	if (current_height == treeHeight)
	{	
		BTLeafNode leaf;
		if (rc = leaf.read(current_pid, pf) < 0) { return rc; }
		if (leaf.insert(key, rid) == RC_NODE_FULL)
		{

			BTLeafNode sibling;

			leaf.insertAndSplit(key, rid, sibling, split_key);

			split_pid = pf.endPid();
			leaf.setNextNodePtr(split_pid);


			if (rc = leaf.write(current_pid, pf) < 0) { return rc; }
			if (rc = sibling.write(split_pid, pf) < 0) { return rc; }

			if (current_height == 1)
			{
				BTNonLeafNode root;
				root.initializeRoot(current_pid, split_key, split_pid);
				rootPid = pf.endPid();
				treeHeight++;
				if (rc = root.write(rootPid, pf) < 0) { return rc; }
				return 0;
			}
			return INSERT_SPLIT;
		}
		return leaf.write(current_pid, pf);
	}

	else
	{
		BTNonLeafNode node;
		PageId child = INVALID_PID;
		int eid;
		if (rc = node.read(current_pid, pf) < 0) { return rc; }
		if (rc = node.locateChildPtr(key, child, eid) < 0) { return rc; }

		rc = insert_R(key, rid, current_height + 1, child, split_key, split_pid);

		if (rc == INSERT_SPLIT)
		{
			if (node.insert(split_key, split_pid) == RC_NODE_FULL)
			{
				BTNonLeafNode sibling_node;
				int mid_key;

				node.insertAndSplit(split_key, split_pid, sibling_node, mid_key);

				split_key = mid_key;
				split_pid = pf.endPid();

				if (rc = node.write(current_pid, pf) < 0) { return rc; }
				if (rc = sibling_node.write(split_pid, pf) < 0) { return rc; }

				if (current_height == 1)
				{
					BTNonLeafNode root;
					root.initializeRoot(current_pid, split_key, split_pid);
					rootPid = pf.endPid();
					treeHeight++;
					if(rc = root.write(rootPid,pf) < 0) { return rc; }
					return 0;
				}
				return INSERT_SPLIT;
			}
			return node.write(current_pid, pf);

		}
	}

	return 0;
}

/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
	if (treeHeight == 0) { return RC_NO_SUCH_RECORD; }

	return locate_R(searchKey, cursor, 1, rootPid);
}

RC BTreeIndex::locate_R(int searchKey, IndexCursor& cursor, int current_height, PageId current_pid)
{
	RC rc;

	if (current_height == treeHeight)
	{
		BTLeafNode leaf;
		int eid;

		if (rc = leaf.read(current_pid, pf) < 0) { return rc; }

		rc = leaf.locate(searchKey, eid);
		cursor.pid = current_pid;
		cursor.eid = eid;
		return rc;
	}
	else
	{
		BTNonLeafNode node;
		int eid;
		PageId child;

		if (rc = node.read(current_pid, pf) < 0) { return rc; }
		if (rc = node.locateChildPtr(searchKey, child, eid) < 0) { return rc; }

		return locate_R(searchKey, cursor, current_height + 1, child);
	}
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
	RC rc;

	BTLeafNode leaf;
	if (cursor.pid == 0) { return LAST_LEAF; }
	if (rc = leaf.read(cursor.pid, pf) < 0) { return rc; }
	if (rc = leaf.readEntry(cursor.eid, key, rid) < 0) { return rc; }

	if (cursor.eid + 1 == leaf.getKeyCount())
	{
		cursor.eid = 0;
		cursor.pid = leaf.getNextNodePtr();
		return 0;
	}
	
	cursor.eid++;
    return 0;
}

void BTreeIndex::dump()
{
	cout << "Tree Height: " << treeHeight << endl;
	cout << "Root PID: " << rootPid << endl;

	BTNonLeafNode root;
	//BTLeafNode root;
	root.read(rootPid, pf);
	root.dump();

	BTNonLeafNode nonleaf1;
	BTNonLeafNode nonleaf2;

	nonleaf1.read(3, pf);
	nonleaf2.read(131, pf);

	nonleaf1.dump();
	nonleaf2.dump();

	BTLeafNode leaf1;
	leaf1.read(1, pf);
	leaf1.dump();

	BTLeafNode leaf2;
	leaf2.read(2, pf);
	leaf2.dump();

	BTLeafNode leaf3;
	leaf3.read(4, pf);
	leaf3.dump();
}
