#include "BTreeNode.h"
#include <iostream>
#include <cstdio>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
using namespace std;

BTLeafNode::BTLeafNode()
{
	fill(buffer, buffer+PageFile::PAGE_SIZE, 0);
	
}

void BTLeafNode::dump()
{
	
	for(int i = 0; i < getKeyCount() * PAIR_SIZE; i += PAIR_SIZE)
	{
		int key, pid, sid;
		memcpy(&key, buffer + i, sizeof(int));
		memcpy(&pid, buffer + i + sizeof(int), sizeof(PageId));
		memcpy(&sid, buffer + i + sizeof(int) + sizeof(PageId), sizeof(int));
		cout << key << " | " << pid << ":" << sid << " || ";
	}
	
	cout << endl;
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{ 
	return pf.read(pid, buffer);
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{ 
	return pf.write(pid, buffer); 
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{ 
	int count = 0;
	char * ptr = buffer;
	int key;
	
	for(int i = 0; i < PageFile::PAGE_SIZE - sizeof(PageId); i += PAIR_SIZE)
	{
		memcpy(&key, ptr + i, sizeof(int)); 
		if (key == 0) { break; }
		count++;
	}
	
	return count;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{ 
	
	if (getKeyCount() == MAX_PAIRS) { return RC_NODE_FULL; }

	int eid;
	locate(key, eid);
	int bytenum = eid * PAIR_SIZE;

	char *new_buf = (char*) calloc(PageFile::PAGE_SIZE, sizeof(char));

	memcpy(new_buf, buffer, bytenum);

	memcpy(new_buf + bytenum, &key, sizeof(int));
	memcpy(new_buf + bytenum + sizeof(int), &rid, sizeof(RecordId));

	memcpy(new_buf + bytenum + PAIR_SIZE, buffer + bytenum, (getKeyCount() - eid) * PAIR_SIZE);

	memcpy(buffer, new_buf, PageFile::PAGE_SIZE - sizeof(PageId));

	free(new_buf);


	return 0; 
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{ 
	int eid;
	locate(key, eid);
	int half = (getKeyCount() + 1) / 2;
	int half_bytenum = half * PAIR_SIZE;

	memcpy(sibling.buffer, buffer + half_bytenum, getKeyCount() * PAIR_SIZE - half_bytenum);
	fill(buffer + half_bytenum, buffer + getKeyCount() * PAIR_SIZE, 0);

	if (eid < half)
	{
		insert(key, rid);
	}
	else 
	{
		sibling.insert(key, rid);
	}

	PageId sibling_pid;
	memcpy(&sibling_pid, sibling.buffer + sizeof(int), sizeof(PageId));

	PageId this_pid;
	this_pid = getNextNodePtr();

	setNextNodePtr(sibling_pid);
	sibling.setNextNodePtr(this_pid);

	memcpy(&siblingKey, sibling.buffer, sizeof(int));

	return 0; 
}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{ 
	int key;
	char *ptr = buffer;

	for (int i = 0; i < getKeyCount() * PAIR_SIZE; i += PAIR_SIZE)
	{
		memcpy(&key, ptr + i, sizeof(int));

		if (key >= searchKey)
		{
			eid = i / PAIR_SIZE;
			if (key != searchKey) { return RC_NO_SUCH_RECORD; }
			else { return 0; } 
		}
	}

	eid = getKeyCount();
	return RC_NO_SUCH_RECORD; 
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{ 

	if (eid > getKeyCount() - 1 || eid < 0)
	{
		return RC_NO_SUCH_RECORD;
	}

	char *ptr = buffer;

	memcpy(&key, ptr + (eid * PAIR_SIZE), sizeof(int));
	memcpy(&rid, ptr + (eid * PAIR_SIZE) + sizeof(int), sizeof(RecordId));

	return 0; 
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{ 
	PageId pid = 0;
	char *ptr = buffer;

	memcpy(&pid, ptr + PageFile::PAGE_SIZE - sizeof(PageId), sizeof(PageId));

	return pid;
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{ 
	if (pid < 0)
	{
		return RC_INVALID_PID;
	}

	char *ptr = buffer;

	memcpy(ptr + PageFile::PAGE_SIZE - sizeof(PageId), &pid, sizeof(PageId));

	return 0;
}

BTNonLeafNode::BTNonLeafNode()
{
	fill(buffer, buffer + PageFile::PAGE_SIZE, 0);
}


/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ return pf.read(pid, buffer); }
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{ return pf.write(pid, buffer); }

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{ 
	int count = 0;
	char * ptr = buffer;
	int key;
	
	for(int i = sizeof(PageId); i < PageFile::PAGE_SIZE - sizeof(PageId); i += PAIR_SIZE)
	{
		memcpy(&key, ptr + i, sizeof(int)); 
		if (key == 0) { break; }
		count++;
	}
	
	return count;
}


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{ 
	if (getKeyCount() == MAX_PAIRS) { return RC_NODE_FULL; }

	int eid;
	PageId dummy_data;
	locateChildPtr(key, dummy_data, eid);
	int bytenum = sizeof(PageId) + eid * PAIR_SIZE;

	char *new_buf = (char*) calloc(PageFile::PAGE_SIZE, sizeof(char));

	memcpy(new_buf, buffer, bytenum);

	memcpy(new_buf + bytenum, &key, sizeof(int));
	memcpy(new_buf + bytenum + sizeof(int), &pid, sizeof(PageId));

	memcpy(new_buf + bytenum + PAIR_SIZE, buffer + bytenum, (getKeyCount() - eid) * PAIR_SIZE);

	memcpy(buffer, new_buf, PageFile::PAGE_SIZE);

	free(new_buf);

	return 0; 
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ 
	int eid;
	PageId dummy_data;
	locateChildPtr(key, dummy_data, eid);
	int bytenum = sizeof(PageId) + eid * PAIR_SIZE;
	char *new_buf = (char*) calloc(PageFile::PAGE_SIZE + PAIR_SIZE, sizeof(char));

	memcpy(new_buf, buffer, bytenum);

	memcpy(new_buf + bytenum, &key, sizeof(int));
	memcpy(new_buf + bytenum + sizeof(int), &pid, sizeof(PageId));

	memcpy(new_buf + bytenum + PAIR_SIZE, buffer + bytenum, (getKeyCount() - eid) * PAIR_SIZE);

	int half = (getKeyCount() + 1) / 2;
	int half_bytenum = sizeof(PageId) + half * PAIR_SIZE;

	
	memcpy(sibling.buffer, new_buf + half_bytenum + sizeof(int), (getKeyCount() + 1) * PAIR_SIZE - half_bytenum);
	memcpy(&midKey, new_buf + half_bytenum, sizeof(int));
	fill(new_buf + half_bytenum, new_buf + (getKeyCount() + 1) * PAIR_SIZE + sizeof(PageId), 0);

	memcpy(buffer, new_buf, PageFile::PAGE_SIZE);

	free(new_buf);

	return 0; 
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid, int& eid)
{ 
	int key, i;
	char *ptr = buffer;

	for (i = sizeof(PageId); i < getKeyCount() * PAIR_SIZE; i += PAIR_SIZE)
	{
		memcpy(&key, ptr + i, sizeof(int));

		if (key > searchKey)
		{
			eid = (i - sizeof(PageId)) / PAIR_SIZE;
			memcpy(&pid, ptr + i - sizeof(PageId), sizeof(PageId));
			return 0;
		}
	}

	eid = getKeyCount();
	memcpy(&pid, ptr + i - sizeof(PageId), sizeof(PageId));
	return 0;
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{
	fill(buffer, buffer + PageFile::PAGE_SIZE, 0);
	memcpy(buffer, &pid1, sizeof(PageId));
	insert(key, pid2);
	return 0; 
}

void BTNonLeafNode::dump()
{
	PageId firstPID;
	memcpy(&firstPID, buffer, sizeof(PageId));
	cout << firstPID << " | ";
	for(int i = sizeof(PageId); i < getKeyCount() * PAIR_SIZE; i += PAIR_SIZE)
	{
		int key, pid;
		memcpy(&key, buffer + i, sizeof(int));
		memcpy(&pid, buffer + i + sizeof(int), sizeof(PageId));
		cout << key << ":" << pid << " | ";
	}
	
	cout << endl;
}