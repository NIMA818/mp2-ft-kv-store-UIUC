/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	short port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TPING;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode,id,port);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){

	memberNode->inited = false;
	memberNode->inGroup = false;
	memberNode->memberList.clear();
	memberNode->heartbeat = 0;
	memberNode->nnb = 0;
	memberNode->bFailed = true;
	return 1;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilities!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
			bool ret = false;

			MessageHdr *msg = (MessageHdr*)data;
			MsgTypes mt = msg->msgType;

			char *msg_data = data + sizeof(MessageHdr);
			int msg_cont_size = (int)(size - sizeof(MessageHdr));


			if(mt == JOINREQ)
			{
				ret = handle_join_req(msg_data,msg_cont_size);
			}
			else if(mt == JOINREP)
			{
				ret = handle_join_rep(msg_data,msg_cont_size);
			}
			else if(mt == PING)
			{
				ret = handle_ping_req(msg_data,msg_cont_size);
			}
			else
			{
				ret = false;
			}
			return ret;
}

bool MP1Node::handle_join_req(char *data,int size)
{
			Address *addr = (Address*)data;					// Address of the requester
			long heart_beat = *((long*)(data+sizeof(Address)+1));
			int id = *((int*)addr->addr);
			short port = *((short*)(addr->addr+4));


			// update membership list....
			MemberListEntry *mle = new MemberListEntry(id, port, heart_beat, par->getcurrtime());
			update_membership_list(mle);


			// send a JOINREP message...
			size_t msg_sz = sizeof(Address)+sizeof(long)+1;
			char* msg = (char*)malloc(msg_sz*sizeof(char));

			MessageHdr *mdr = (MessageHdr*)msg;
			mdr->msgType = JOINREP;
			char *st = msg+sizeof(MessageHdr);

			memcpy(st,(char*)&(memberNode->addr.addr),sizeof(Address));
			memcpy(st+sizeof(Address)+1,(char*)&(memberNode->heartbeat),sizeof(long));

			emulNet->ENsend(&(memberNode->addr), addr, (char *)msg, msg_sz);

			free(msg);

			return true;
}


bool MP1Node::handle_join_rep(char *data,int size)
{

			memberNode->inGroup = true;						// Added in group

			Address *addr = (Address*)data;

			int id = *(int*)(addr->addr);
			short port = *(short*)(&addr->addr[4]);
			long heart_beat = *(long*)(data+sizeof(Address)+1);

			MemberListEntry *mle = new MemberListEntry(id,port,heart_beat,par->getcurrtime());
			update_membership_list(mle);

			return true;
}


bool MP1Node::handle_ping_req(char *data,int size)
{
		size_t entry_len = 15;
		int num_ent = size/entry_len;

		for(int i=0;i<num_ent;i++)
		{
			Address *addr = (Address*)data;
			long heart_beat = *(long*)(data+sizeof(Address)+1);

			int id = *(int*)(&addr->addr);
			short port = *(short*)(&addr->addr[4]);
			MemberListEntry *mle = new MemberListEntry(id, port, heart_beat, par->getcurrtime());
			update_membership_list(mle);

			data = data + entry_len;
		}
		return true;
}

static Address get_address(MemberListEntry *entry){
		Address address;
    	memcpy(&address.addr, &(entry->id), sizeof(int));
    	memcpy(&address.addr[4], &(entry->port), sizeof(short));
    	return address;
}

void MP1Node::update_membership_list(MemberListEntry *mle)
{

		Address oth_addr = get_address(mle);

		if(oth_addr == memberNode->addr)return;

		long heart_beat = mle->getheartbeat();

		vector<MemberListEntry>::iterator it;
		for(it = memberNode->memberList.begin()+1; it!=memberNode->memberList.end(); it++)
		{
			Address this_addr = get_address(&(*it));
			if(this_addr == oth_addr)
			{
				long this_beat = it->getheartbeat();

				if(heart_beat == -1)				// someone found it failed....
				{
					it->setheartbeat(-1);
					return;
				}
				if(this_beat == -1)					// I have marked it failed before....
				{
					return;
				}
				if(this_beat < heart_beat)
				{
					it->setheartbeat(heart_beat);
					it->settimestamp(par->getcurrtime());
					return;
				}
				return;
			}
		}
		if(heart_beat == -1)					// It's removed before....
		{
			return;
		}
		else									// new member....
		{
			mle->settimestamp(par->getcurrtime());
			memberNode->memberList.push_back(*mle);

			#ifdef DEBUGLOG
				log->logNodeAdd(&(memberNode->addr), &oth_addr);
			#endif
		}

}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps(){

		if(memberNode->pingCounter == 0)
		{
			memberNode->heartbeat++;
			memberNode->memberList[0].setheartbeat(memberNode->heartbeat);
			memberNode->memberList[0].setheartbeat(par->getcurrtime());

			disseminate_membership_list();

			memberNode->pingCounter = TPING;
		}
		else
		{
			memberNode->pingCounter--;
		}

		check_failure();

    	return;
}


void MP1Node::check_failure()
{
		Address oth_addr;
		int curr_time = par->getcurrtime();

		for(auto it = memberNode->memberList.begin()+1; it!= memberNode->memberList.end();it++)
		{
			int ent_time = it->gettimestamp();

			if(ent_time + TREMOVE < curr_time)
			{
				#ifdef DEBUGLOG
					oth_addr = get_address(&(*it));
					log->logNodeRemove(&memberNode->addr, &oth_addr);
            	#endif

				memberNode->memberList.erase(it);
				it--;
				continue;
			}

			if(ent_time + TFAIL < curr_time)
			{
				it->setheartbeat(-1);
			}

		}
}

void MP1Node::disseminate_membership_list()
{
			int sz=0;
			char* msg = serializeMembershipList(sz);

			MessageHdr *mdr = (MessageHdr*)msg;
			mdr->msgType = PING;
			for(auto it = memberNode->memberList.begin()+1; it!= memberNode->memberList.end(); it++)
			{
				Address addr = get_address(&(*it));
				emulNet->ENsend(&memberNode->addr, &addr, (char *)msg, sz);
			}

			free(msg);
}

char* MP1Node::serializeMembershipList(int& size)
{

			size_t num_ent = memberNode->memberList.size();

			size_t sz = sizeof(MessageHdr)+((sizeof(Address)+sizeof(long)+1)*num_ent);
			size  = (int)sz;

			char *msg = (char*)malloc(sizeof(char)*sz);

			char* st = (msg+sizeof(MessageHdr));

			size_t ent_size = 15;

			for(auto it = memberNode->memberList.begin(); it!= memberNode->memberList.end(); it++)
			{
				Address adr = get_address(&(*it));
				memcpy(st,(char*)&(adr.addr),sizeof(Address));
				memcpy(st+sizeof(Address)+1,(char*)&it->heartbeat,sizeof(long));

				st = st+ent_size;
			}

			return (char*)msg;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode, int id, short port) {

	memberNode->memberList.clear();
	MemberListEntry *mle = new MemberListEntry(id, port, memberNode->heartbeat, par->getcurrtime());
	memberNode->memberList.push_back(*mle);
	memberNode->myPos = memberNode->memberList.begin();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
