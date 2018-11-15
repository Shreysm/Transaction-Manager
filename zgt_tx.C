/***************** Transaction class **********************/
/*** Implements methods that handle Begin, Read, Write, ***/
/*** Abort, Commit operations of transactions. These    ***/
/*** methods are passed as parameters to threads        ***/
/*** spawned by Transaction manager class.              ***/
/**********************************************************/
/*CSE 5331 PROJECT 2
*Author-Kanthi Komar and Shreyas Mohan
*/
/* Required header files */
#include <stdio.h>
#include <stdlib.h>
#include <sys/signal.h>
#include "zgt_def.h"
#include "zgt_tm.h"
#include "zgt_extern.h"
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <pthread.h>


extern void *start_operation(long, long);  //starts opeartion by doing conditional wait
extern void *finish_operation(long);       // finishes abn operation by removing conditional wait
extern void *open_logfile_for_append();    //opens log file for writing
extern void *do_commit_abort(long, char);   //commit/abort based on char value (the code is same for us)

extern zgt_tm *ZGT_Sh;			// Transaction manager object

FILE *logfile; //declare globally to be used by all

/* Transaction class constructor */
/* Initializes transaction id and status and thread id */
/* Input: Transaction id, status, thread id */

zgt_tx::zgt_tx( long tid, char Txstatus,char type, pthread_t thrid){
  this->lockmode = (char)' ';  //default
  this->Txtype = type; //Fall 2014[jay] R = read only, W=Read/Write
  this->sgno =1;
  this->tid = tid;
  this->obno = -1; //set it to a invalid value
  this->status = Txstatus;
  this->pid = thrid;
  this->head = NULL;
  this->nextr = NULL;
  this->semno = -1; //init to  an invalid sem value
}

/* Method used to obtain reference to a transaction node      */
/* Inputs the transaction id. Makes a linear scan over the    */
/* linked list of transaction nodes and returns the reference */
/* of the required node if found. Otherwise returns NULL      */

zgt_tx* get_tx(long tid1){  
  zgt_tx *txptr, *lastr1;
  
  if(ZGT_Sh->lastr != NULL){	// If the list is not empty
      lastr1 = ZGT_Sh->lastr;	// Initialize lastr1 to first node's ptr
      for  (txptr = lastr1; (txptr != NULL); txptr = txptr->nextr)
	    if (txptr->tid == tid1) 		// if required id is found									
	       return txptr; 
      return (NULL);			// if not found in list return NULL
   }
  return(NULL);				// if list is empty return NULL
}

/* Method that handles "BeginTx tid" in test file     */
/* Inputs a pointer to transaction id, obj pair as a struct. Creates a new  */
/* transaction node, initializes its data members and */
/* adds it to transaction list */

void *begintx(void *arg){
  //Initialize a transaction object. Make sure it is
  //done after acquiring the semaphore for the tm and making sure that 
  //the operation can proceed using the condition variable. when creating
  //the tx object, set the tx to TR_ACTIVE and obno to -1; there is no 
  //semno as yet as none is waiting on this tx.
  
    struct param *node = (struct param*)arg;// get tid and count
    start_operation(node->tid, node->count); 
    zgt_tx *tx = new zgt_tx(node->tid,TR_ACTIVE, node->Txtype, pthread_self());	// Create new tx node
    open_logfile_for_append();
    fprintf(logfile, "T%d\t%c \tBeginTx\n", node->tid, node->Txtype);	// Write log record and close
    fflush(logfile);
  
    zgt_p(0);				// Lock Tx manager; Add node to transaction list
  
    tx->nextr = ZGT_Sh->lastr;
    ZGT_Sh->lastr = tx;   
    zgt_v(0); 			// Release tx manager 
  
  finish_operation(node->tid);
  pthread_exit(NULL);				// thread exit

}

/* Method to handle Readtx action in test file    */
/* Inputs a pointer to structure that contians     */
/* tx id and object no to read. Reads the object  */
/* if the object is not yet present in hash table */
/* or same tx holds a lock on it. Otherwise waits */
/* until the lock is released */
//Read Operation
void *readtx(void *arg) {
	
	struct param *node = (struct param*)arg;
	// start_operation to make the transaction run in correct order
	start_operation(node->tid, node->count); 
	//Getting the transaction ID
	zgt_tx *tx = get_tx(node->tid); 
	//Setting the lock 
	//Use s: shared ;lock for read only operation
	tx->set_lock(node->tid, 1,node->obno,node->count, 'S'); 
	finish_operation(node->tid); 
	//Exit Thread
    pthread_exit(NULL); 
}

//do the operations for writing,similar to readTx
void *writetx(void *arg) { //do the operations for writing; similar to readTx
	
	
	struct param *node = (struct param*)arg;	
	start_operation(node->tid, node->count);
	//Get Transaction ID
	zgt_tx *tx = get_tx(node->tid);
	//Setting the lock
	// use x: exclusive lock for read/Write operation
	tx->set_lock(node->tid, 1,node->obno,node->count,'X');
	finish_operation(node->tid);
	//Exit Thread
    pthread_exit(NULL); 
}

void *aborttx(void *arg){
        struct param *node = (struct param*)arg;
        // get the Transaction ID
        start_operation(node->tid, node->count);
        // p operation with semno 0 for locking transaction manager
        zgt_p(0);       
        // Abort is denoted by 'A'
        do_commit_abort(node->tid,'A'); 
        // v operation with semno 0 for releasing the lock on transaction manager
        zgt_v(0);       

        finish_operation(node->tid);
        pthread_exit(NULL);     // thread exit
}

void *committx(void *arg){
        struct param *node = (struct param*)arg;
        // get tid and count
        start_operation(node->tid, node->count);
        // Lock Transaction manager
        zgt_p(0);       

        zgt_tx *currentTx = get_tx(node->tid);
        // Commit is denoted by 'E'
        currentTx->status = 'E';  

        printf("Semno-> inside committx :: %d\n", currentTx->semno);

        do_commit_abort(node->tid,'E');
        // Release transaction manager
        zgt_v(0);       
        finish_operation(node->tid);
        pthread_exit(NULL);     // thread exit
}

      // called from commit/abort with appropriate parameter to do the actual
      // operation. Make sure you give error messages if you are trying to
      // commit/abort a non-existant tx

void *do_commit_abort(long t, char status){
      int i =0;

        // Get Current Transaction 
        zgt_tx *currentTx=get_tx(t);
        currentTx->print_tm();

        if(currentTx == NULL){
        //Transaction doesn't exist
            fprintf(logfile, "\t Transaction %d doesn't exist. \n", t);
            fflush(logfile);
      }else{
      // frees all locks held on the object by current transaction
            currentTx->free_locks();
            //v operation on semaphore
            zgt_v(currentTx->tid);
            //Remove the transaction node from TM
            int check_semno = currentTx->semno;
            currentTx->end_tx();

            

            /* Tell all the waiting transactions in queue about release of locks by current Txn*/
            if(check_semno > -1){
              int noOfTransaction = zgt_nwait(check_semno); //No of txns waiting on the current txn
              printf("numberOfTransaction:: %d currentTx->Semno %d \n", noOfTransaction, check_semno);

              if(noOfTransaction > 0){
            // Releases all the locks(as many as the number of transactions in the waiting queue) by doing v operation
          
                  while(i <= noOfTransaction)
                  {
                    zgt_v(check_semno);
                    i=i+1;
                  }
            //Release all semaphores of waiting txns in queue

                
              }
            }else{
              printf("\ncheck_semno is -1\n");
            }


          // Writing on to the log file
            if(status== 'A'){
              fprintf(logfile, "T%d\t  \tAbortTx \t \n",t);
              fflush(logfile);
            }else if(status == 'E'){
              fprintf(logfile, "T%d\t  \tCommitTx \t \n",t);
              fflush(logfile);
            }
          
        }
}     

int zgt_tx::remove_tx ()
{
  //remove the transaction from the TM
  
  zgt_tx *txptr, *lastr1;
  lastr1 = ZGT_Sh->lastr;
  for(txptr = ZGT_Sh->lastr; txptr != NULL; txptr = txptr->nextr){	// scan through list
	  if (txptr->tid == this->tid){		// if req node is found          
		 lastr1->nextr = txptr->nextr;	// update nextr value; done
		 //delete this;
         return(0);
	  }
	  else lastr1 = txptr->nextr;			// else update prev value
   }
  fprintf(logfile, "Trying to Remove a Tx:%d that does not exist\n", this->tid);
  fflush(logfile);
  printf("Trying to Remove a Tx:%d that does not exist\n", this->tid);
  fflush(stdout);
  return(-1);
}

/* this method sets lock on objno1 with lockmode1 for a tx in this*/

int zgt_tx::set_lock(long tid1, long sgno1, long obno1, int count, char lockmode1) 
{
  //if the thread has to wait, block the thread on a semaphore from the
  //sempool in the transaction manager. Set the appropriate parameters in the
  //transaction list if waiting.
  //if successful  return(0);
  //Get the Transaction
    zgt_tx *tx = get_tx(tid1);
    zgt_p(0);
  /*Check if the current transaction is already present in the hashtable, which means that it has already acquired a lock*/
    zgt_hlink *currentTx = ZGT_Ht->find(sgno1, obno1);
    zgt_v(0);
  /*If the currentTx is not present in the hash table, we have to add it to the hash table*/
    if(currentTx == NULL){
        zgt_p(0);
  //Adding currentTx to the hash table.
        ZGT_Ht->add(tx,sgno1, obno1, lockmode1);
        zgt_v(0);
  //calling perform_readWrite function to simulate read and write operations
        tx->perform_readWrite(tid1, obno1, lockmode1);
    }
    else if(tx->tid == currentTx->tid){
  //Since the current transaction is already in hash table and holds the lock, perform read/write
        tx->perform_readWrite(tid1, obno1, lockmode1);
    }
  //When the object is locked by an other transaction.
    else{
        
  //Lock the Transaction Manager
    zgt_p(0);
  //Find the Transaction
        zgt_hlink *linkp = ZGT_Ht->findt(tid1, sgno1, obno1);
  //Release the lock on the Transaction Manager.
        zgt_v(0);
        if(linkp!=NULL){
  //If linkp is found, perform read/write
            tx->perform_readWrite(tid1, obno1, lockmode1);
        }
        else{
  //If Current Transaction does not hold any locks
            printf("T%d does not hold any lock.\n",currentTx->tid);
            fflush(stdout);
            int noOfTxWaiting = zgt_nwait(currentTx->tid);
            printf(":::Current Tx lock mode : %c , Old Tx lock mode : %c ,No of Transactions Waiting : %d \n",lockmode1,currentTx->lockmode,noOfTxWaiting);
            fflush(stdout);
      
	/*check all the conditions when the transaction has t*/
  
            if(lockmode1 == 'X' || (lockmode1 == 'S' && currentTx->lockmode == 'X' )
               || (lockmode1 == 'S' && currentTx->lockmode == 'S' && noOfTxWaiting>0)){
                tx->obno = obno1;
                tx->lockmode = lockmode1;
                tx->status = TR_WAIT;
  //Setting the Semaphore to the transaction id.
                tx->setTx_semno(currentTx->tid,currentTx->tid);
                printf(":::Tx%d is waiting on Tx%d for object no %d \n",tid1,currentTx->tid,obno);
                fflush(stdout);
  //Locking the current transaction
                zgt_p(currentTx->tid);
  //Making the transaction active and continuing it.
                tx->obno = -1;
                tx->lockmode = ' ';
                tx->status = TR_ACTIVE;
                printf(":::Tx%d is waited on Tx%d for object no %d is continuing \n",tid1,currentTx->tid,obno);
                fflush(stdout);
  //Once the transaction is active, perform read/write.
                tx->perform_readWrite(tid1, obno1, lockmode1);
  //Release the lock on the Transaction.
                zgt_v(currentTx->tid);
            }
            else{
  //Lock requested is a Shared lock and no other transaction hold the shared lock.
  //So perform the read
                tx->perform_readWrite(tid1, obno1, lockmode1);
            }
        }
    }
    return(0);
}

// routine to perform the acutual read/write operation
// based  on the lockmode
void zgt_tx::perform_readWrite(long tid,long obno, char lockmode){
    int i;
    
    
    int objectValue = ZGT_Sh->objarray[obno]->value;
    
    if(lockmode == 'S') {
    //For Read Operation
        ZGT_Sh->objarray[obno]->value=objectValue - 1;  // decrease value of the object by 1
		open_logfile_for_append();
		fprintf(logfile, "T%d\t      \tReadTx\t\t%d:%d:%d\t\tReadLock\tGranted\t\t %c\n",
                this->tid, obno, ZGT_Sh->objarray[obno]->value, ZGT_Sh->optime[tid], this->status);
        fflush(logfile);
        for(i=0; i<ZGT_Sh->optime[tid]*50; i++) {
			//empty loop to simulate the read operation
      //sleep(tid);
    }
    }
    // For Write Operation
	else if(lockmode == 'X')   
    {
        ZGT_Sh->objarray[obno]->value=objectValue + 1;  // increase value of the object by 1
		//logging to file
		open_logfile_for_append();
        fprintf(logfile, "T%d\t      \tWriteTx\t\t%d:%d:%d\t\t\tWriteLock\tGranted\t\t %c\n",
               this->tid, obno, ZGT_Sh->objarray[obno]->value, ZGT_Sh->optime[tid], this->status);
        fflush(logfile);
        for(i=0; i<ZGT_Sh->optime[tid]*50; i++) {
            //sleep(tid);
			//empty loop to simulate the write operation
    }
    }
}

// this part frees all locks owned by the transaction
// Need to free the thread in the waiting queue
// try to obtain the lock for the freed threads
// if the process itself is blocked, clear the wait and semaphores

int zgt_tx::free_locks()
{
  zgt_hlink* temp = head;  //first obj of tx
  
  open_logfile_for_append();
   
  for(temp;temp != NULL;temp = temp->nextp){	// SCAN Tx obj list

      //fprintf(logfile, "%d : %d \t", temp->obno, ZGT_Sh->objarray[temp->obno]->value);
      //fflush(logfile);
      
      if (ZGT_Ht->remove(this,1,(long)temp->obno) == 1){
	   printf(":::ERROR:node with tid:%d and onjno:%d was not found for deleting", this->tid, temp->obno);		// Release from hash table
	   fflush(stdout);
      }
      else {
#ifdef TX_DEBUG
	   printf("\n:::Hash node with Tid:%d, obno:%d lockmode:%c removed\n",
                            temp->tid, temp->obno, temp->lockmode);
	   fflush(stdout);
#endif
      }
    }
  fprintf(logfile, "\n");
  fflush(logfile);
  
  return(0);
}		

// CURRENTLY Not USED
// USED to COMMIT
// remove the transaction and free all associate dobjects. For the time being
// this can be used for commit of the transaction.

int zgt_tx::end_tx()  //2014: not used
{
  zgt_tx *linktx, *prevp;

  linktx = prevp = ZGT_Sh->lastr;
  
  while (linktx){
    if (linktx->tid  == this->tid) break;
    prevp  = linktx;
    linktx = linktx->nextr;
  }
  if (linktx == NULL) {
    printf("\ncannot remove a Tx node; error\n");
    fflush(stdout);
    return (1);
  }
  if (linktx == ZGT_Sh->lastr) ZGT_Sh->lastr = linktx->nextr;
  else {
    prevp = ZGT_Sh->lastr;
    while (prevp->nextr != linktx) prevp = prevp->nextr;
    prevp->nextr = linktx->nextr;    
  }
}

// currently not used
int zgt_tx::cleanup()
{
  return(0);
  
}

// check which other transaction has the lock on the same obno
// returns the hash node
zgt_hlink *zgt_tx::others_lock(zgt_hlink *hnodep, long sgno1, long obno1)
{
  zgt_hlink *ep;
  ep=ZGT_Ht->find(sgno1,obno1);
  while (ep)				// while ep is not null
    {
      if ((ep->obno == obno1)&&(ep->sgno ==sgno1)&&(ep->tid !=this->tid)) 
	return (ep);			// return the hashnode that holds the lock
      else  ep = ep->next;		
    }					
  return (NULL);			//  Return null otherwise 
  
}

// routine to print the tx list
// TX_DEBUG should be defined in the Makefile to print

void zgt_tx::print_tm(){
  
  zgt_tx *txptr;
  
#ifdef TX_DEBUG
  printf("printing the tx list \n");
  printf("Tid\tTxType\tThrid\t\tobjno\tlock\tstatus\tsemno\n");
  fflush(stdout);
#endif
  txptr=ZGT_Sh->lastr;
  while (txptr != NULL) {
#ifdef TX_DEBUG
    printf("%d\t%c\t%d\t%d\t%c\t%c\t%d\n", txptr->tid, txptr->Txtype, txptr->pid, txptr->obno, txptr->lockmode, txptr->status, txptr->semno);
    fflush(stdout);
#endif
    txptr = txptr->nextr;
  }
  fflush(stdout);
}

//currently not used
void zgt_tx::print_wait(){

  //route for printing for debugging
  
  printf("\n    SGNO        TxType       OBNO        TID        PID         SEMNO   L\n");
  printf("\n");
}
void zgt_tx::print_lock(){
  //routine for printing for debugging
  
  printf("\n    SGNO        OBNO        TID        PID   L\n");
  printf("\n");
  
}



// routine that sets the semno in the Tx when another tx waits on it.
// the same number is the same as the tx number on which a Tx is waiting
int zgt_tx::setTx_semno(long tid, int semno){
  zgt_tx *txptr;
  
  txptr = get_tx(tid);
  if (txptr == NULL){
    printf("\n:::ERROR:Txid %d wants to wait on sem:%d of tid:%d which does not exist\n", this->tid, semno, tid);
    fflush(stdout);
    return(-1);
  }
  if (txptr->semno == -1){
    txptr->semno = semno;
    return(0);
  }
  else if (txptr->semno != semno){
#ifdef TX_DEBUG
    printf(":::ERROR Trying to wait on sem:%d, but on Tx:%d\n", semno, txptr->tid);
    fflush(stdout);
#endif
    exit(1);
  }
  return(0);
}

// routine to start an operation by checking the previous operation of the same
// tx has completed; otherwise, it will do a conditional wait until the
// current thread signals a broadcast

void *start_operation(long tid, long count){
  
  pthread_mutex_lock(&ZGT_Sh->mutexpool[tid]);	// Lock mutex[t] to make other
  // threads of same transaction to wait
  
  while(ZGT_Sh->condset[tid] != count)		// wait if condset[t] is != count
    pthread_cond_wait(&ZGT_Sh->condpool[tid],&ZGT_Sh->mutexpool[tid]);
  
}

// Otherside of the start operation;
// signals the conditional broadcast

void *finish_operation(long tid){
  ZGT_Sh->condset[tid]--;	// decr condset[tid] for allowing the next op
  pthread_cond_broadcast(&ZGT_Sh->condpool[tid]);// other waiting threads of same tx
  pthread_mutex_unlock(&ZGT_Sh->mutexpool[tid]); 
}

void *open_logfile_for_append(){
  
  if ((logfile = fopen(ZGT_Sh->logfile, "a")) == NULL){
    printf("\nCannot open log file for append:%s\n", ZGT_Sh->logfile);
    fflush(stdout);
    exit(1);
  }
}
