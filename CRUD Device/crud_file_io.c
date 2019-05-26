////////////////////////////////////////////////////////////////////////////////
//
//  File           : crud_file_io.h
//  Description    : This is the implementation of the standardized IO functions
//                   for used to access the CRUD storage system.
//
//  Author         : Patrick McDaniel
//  Last Modified  : Mon Oct 20 12:38:05 PDT 2014
//

// Includes
#include <malloc.h>
#include <string.h>

// Project Includes
#include <crud_file_io.h>
#include <cmpsc311_log.h>
#include <cmpsc311_util.h>
#include <crud_network.h>

// Defines
#define CIO_UNIT_TEST_MAX_WRITE_SIZE 1024
#define CRUD_IO_UNIT_TEST_ITERATIONS 10240

// Other definitions

//typedef struct {
//	char      filename[CRUD_MAX_PATH_LENGTH]; // The filename of the data to be manipulated
//	CrudOID   object_id;                      // The handle of the object
//	uint32_t  position;                       // This is the position of the file
//	uint32_t  length;                         // This is the length of the file
//	uint8_t   open;                           // Flag indicating the file is currently open
//} CrudFileAllocationType;

// Type for UNIT test interface
typedef enum {
	CIO_UNIT_TEST_READ   = 0,
	CIO_UNIT_TEST_WRITE  = 1,
	CIO_UNIT_TEST_APPEND = 2,
	CIO_UNIT_TEST_SEEK   = 3,
} CRUD_UNIT_TEST_TYPE;

// File system Static Data
// This the definition of the file table
CrudFileAllocationType crud_file_table[CRUD_MAX_TOTAL_FILES]; // The file handle table

// Pick up these definitions from the unit test of the crud driver
CrudRequest construct_crud_request(CrudOID oid, CRUD_REQUEST_TYPES req,
		uint32_t length, uint8_t flags, uint8_t res);
int deconstruct_crud_request(CrudRequest request, CrudOID *oid,
		CRUD_REQUEST_TYPES *req, uint32_t *length, uint8_t *flags,
		uint8_t *res);

int init = 0;

//
// Implementation

//////////////////////////////////////////////////////////////////////////////////
//Function : Breaks down a CrudResponse into the desired information
//IN: a CrudResponse, and pointers to the wanted informatino
//Out: OID, Size, and the result
void decryptResponse(CrudResponse response, CrudOID *ID, int32_t *size, int *result){

    // Parse parseThis for each of five fields 
*ID = (uint32_t) (response >> 32); // obtain bits 32-63 
*size = (uint32_t) ((response >> 4) & 0xFFFFFF); // obtain bits 4-27 
*result = (uint8_t) (response & 1); // obtain bit 0 


return;
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_format
// Description  : This function formats the crud drive, and adds the file
//                allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

uint16_t crud_format(void) {
	CrudOID ID;
	int32_t length;
	int result;
	CrudRequest request;
	CrudResponse response;

	if(init==0){
	        request = construct_crud_request(0,CRUD_INIT, 0, 0,0);
	        response = crud_client_operation(request,NULL); 
	        decryptResponse(response,&ID,&length, &result);// decrypt response

		if(result != 0)
			return -1;
	
	init = 1;
	}

	//format
	request = construct_crud_request(0,CRUD_FORMAT, 0, CRUD_NULL_FLAG,0);
	response = crud_client_operation(request,NULL); 
	decryptResponse(response,&ID,&length, &result);// decrypt response
	
	//check to see if format was successful	
	if(result != 0)
		return-1;

	//initializes table with zeros
	for(int i = 0; i < CRUD_MAX_TOTAL_FILES; i++){
		strcpy(crud_file_table[i].filename, ""); 
		crud_file_table[i].object_id= 0;                      
		crud_file_table[i].position= 0;                       
		crud_file_table[i].length = 0;                        
		crud_file_table[i].open = 0;  
	}

	
	
	//creates priority object for storing the file table
	request = construct_crud_request(0, CRUD_CREATE, CRUD_MAX_TOTAL_FILES*sizeof(CrudFileAllocationType), CRUD_PRIORITY_OBJECT,0);
	response = crud_client_operation(request,crud_file_table); 
	decryptResponse(response,&ID,&length, &result);// decrypt response
	
	if(result !=0)
		return -1;
	
	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... formatting complete.");
	return(0);
	
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_mount
// Description  : This function mount the current crud file system and loads
//                the file allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

uint16_t crud_mount(void) {
CrudOID ID;
int32_t length=0;
int result;
CrudRequest request;
CrudResponse response;
//
//local variables
//
	if(init == 0){
		request = construct_crud_request(0, CRUD_INIT, 0, 0,0);
		response = crud_client_operation(request,NULL); 
		decryptResponse(response,&ID,&length, &result);	
		if(result!=0)
			return -1;
		init = 1;
    	}

	request = construct_crud_request(0, CRUD_READ, CRUD_MAX_TOTAL_FILES*sizeof(CrudFileAllocationType), CRUD_PRIORITY_OBJECT,0);
	response = crud_client_operation(request,crud_file_table); 
	decryptResponse(response,&ID,&length, &result);// decrypt response
	
	if(result !=0)
		return -1;

	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... mount complete.");
	return(0);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_unmount
// Description  : This function unmounts the current crud file system and
//                saves the file allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

uint16_t crud_unmount(void) {
CrudOID ID;
int32_t length=0;
int result;
CrudRequest request;
CrudResponse response;
//
//local variables
//	
	if(init == 0)
		return -1;

	request = construct_crud_request(0, CRUD_UPDATE,CRUD_MAX_TOTAL_FILES*sizeof(CrudFileAllocationType), CRUD_PRIORITY_OBJECT,0);
	response = crud_client_operation(request,crud_file_table); 
	decryptResponse(response,&ID,&length, &result);// decrypt response
	if(result !=0)
		return -1;

	request = construct_crud_request(0,CRUD_CLOSE,0,CRUD_NULL_FLAG,0);
	response = crud_client_operation(request,NULL);
	decryptResponse(response,&ID,&length, &result);
	if(result !=0)
		return -1;

	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... unmount complete.");
	return (0);
}

//////////////////////////////////////////////////////////////////////////////////
//Function : Forms a CrudRequest from the given information
//IN: a pointer to a CrudRequest, and info to form a request
//Out: a CrudRequest
void formRequest(CrudRequest *request,int32_t OID, int task, int32_t size){

	int64_t temp = 0x0000;// Creates a blank 64 bit numberf

	//takes the given information and shifts it to the left to meet the desired format of a CrudRequest
	int64_t a = OID; 
	a= a<<32;
	int64_t b = task;
	b= b<<28;
	int64_t c = size;
	c = c<<4;

	// combines the numbers into one 64 bit number
	temp = temp | a;
	temp = temp | b;
	temp = temp | c;

	//finalizes number to the request
	*request = temp;
	return;

}



//////////////////////////////////////////////////////////////////////////////////
//Function : checks to see if the file is open
//IN: file description
//Out: i is it is open; 0 if it is closed
int openCheck(int16_t fd){

	//checks for the open marker in storage
	if (crud_file_table[fd].open!=1)
		return 1;
	else 
		return 0;
}


//
// Implementation

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_open
// Description  : This function opens the file and returns a file handle
//
// Inputs       : path - the path "in the storage array"
// Outputs      : file handle if successful, -1 if failure

int16_t crud_open(char *path) {
CrudOID ID;
int32_t length=0;
int result;
CrudRequest request;
CrudResponse response;
//
//Local variables
//

//check to see if the Crud system is initialized
// if not then it initialies the system
	if(init == 0){
		request = construct_crud_request(0, CRUD_INIT, 0, 0,0);
		response = crud_client_operation(request,NULL); 
		decryptResponse(response,&ID,&length, &result);	
		if(result!=0)
			return -1;
		init = 1;
    	}


// if the request was successful, finds an open index in storage

 
	int x = 0;
	while( x < CRUD_MAX_TOTAL_FILES && (strcmp(crud_file_table[x].filename, path)!=0))
		x++;

//stores information about object in the found index
	if (x == CRUD_MAX_TOTAL_FILES){
        	// Assign file new slot in crud_file_table

        	x = 0;
       		// Search through file table until an empty slot is found
        	while (strcmp(crud_file_table[x].filename, "") != 0)
            		x++;

        	// Copy path into filename 
        	strcpy(crud_file_table[x].filename, path);
        
        	// Set initial contents to empty
        	crud_file_table[x].object_id = 0;
       	 	crud_file_table[x].position = 0;
        	crud_file_table[x].length = 0;
        	crud_file_table[x].open = 1;
    	}
	//else, the file already exists
	else{
		crud_file_table[x].position = 0;
		crud_file_table[x].open = 1;
	}

	
	return x;//returns array index

}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_close
// Description  : This function closes the file
//
// Inputs       : fd - the file handle of the object to close
// Outputs      : 0 if successful, -1 if failure

int16_t crud_close(int16_t fh) {
CrudOID ID;
int32_t length=0;
int result;
CrudRequest request;
CrudResponse response;

	
	//check to see if the Crud system is initialized
	// if not then it initialies the system
	if(init == 0){
		request = construct_crud_request(0, CRUD_INIT, 0, 0,0);
		response = crud_client_operation(request,NULL); 
		decryptResponse(response,&ID,&length, &result);	
		if(result!=0)
			return -1;
		init = 1;
    	}

	//check if the file is open
	if(crud_file_table[fh].open==0)
		return -1;

	// change the open marker to 0 and return 0

	crud_file_table[fh].open = 0;
	return 0;


}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_read
// Description  : Reads up to "count" bytes from the file handle "fh" into the
//                buffer  "buf".
//
// Inputs       : fd - the file descriptor for the read
//                buf - the buffer to place the bytes into
//                count - the number of bytes to read
// Outputs      : the number of bytes read or -1 if failures

int32_t crud_read(int16_t fd, void *buf, int32_t count) {
CrudOID ID;
int32_t length=0;
int result;
CrudRequest request;
CrudResponse response;

int pos = crud_file_table[fd].position;
int amtRead = 0;
void *temp= malloc(crud_file_table[fd].length);
//
//local variables
//

	//check to see if the Crud system is initialized
	// if not then it initialies the system
	if(init == 0){
		request = construct_crud_request(0, CRUD_INIT, 0, 0,0);
		response = crud_client_operation(request,NULL); 
		decryptResponse(response,&ID,&length, &result);	
		if(result!=0)
			return -1;
		init = 1;
    	}

	//check to see is the file is open 
	if(crud_file_table[fd].open == 0)
		return -1;	
	
	if(crud_file_table[fd].object_id != 0){//check to see if the file has an OID

		request = construct_crud_request(crud_file_table[fd].object_id,CRUD_READ,crud_file_table[fd].length,0,0);//create a read request
		response = crud_client_operation(request,temp);// submit request with the temp buffer
		decryptResponse(response,&ID,&length, &result);// decrypt response

		if(result == 0){//if request was successful

			//copy temp[pos] to buf for 'count' bits
			if (count > crud_file_table[fd].length - crud_file_table[fd].position)
				memcpy(buf, temp+crud_file_table[fd].position, crud_file_table[fd].length - crud_file_table[fd].position);
			else	
				memcpy(buf, temp+crud_file_table[fd].position, count);
		
			// determine the amount read to the passed buffer then adjust the position of the file 
			if(length<pos+count){
				amtRead= length-pos;
				crud_file_table[fd].position = crud_file_table[fd].length;
			}

			else{
				amtRead= count;	
				crud_file_table[fd].position += count;
			}
		}

	free(temp);// free the temp buffer
	}

	else 
		return -1;


	return amtRead;
}

/////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_write
// Description  : Writes "count" bytes to the file handle "fh" from the
//                buffer  "buf"
//
// Inputs       : fd - the file descriptor for the file to write to
//                buf - the buffer to write
//                count - the number of bytes to write
// Outputs      : the number of bytes written or -1 if failure

int32_t crud_write(int16_t fd, void *buf, int32_t count){

CrudOID ID;
int32_t length =0;
int result;
CrudRequest request;
CrudResponse response;

//
//local variables
//

	//check to see if the Crud system is initialized
	// if not then it initialies the system
	if(init == 0){
		request = construct_crud_request(0, CRUD_INIT, 0, 0,0);
		response = crud_client_operation(request,NULL); 
		decryptResponse(response,&ID,&length, &result);	
		if(result!=0)
			return -1;
		init = 1;
    	}

	//check to see if file is open
	if(!crud_file_table[fd].open)
		return -1;

	if(crud_file_table[fd].object_id == 0){// check to see if file has an OID
		request = construct_crud_request(0,CRUD_CREATE,count,0,0);//create read request
		response = crud_client_operation(request,buf);//submit request
		decryptResponse(response,&ID,&length, &result);	//decypher request
		if(result !=0)
			return -1;

		//update information of the file at the index
		crud_file_table[fd].object_id = ID;
		crud_file_table[fd].position = count;	
		crud_file_table[fd].length = count;

		return count;// return amount wrote
	}

	else{
		request = construct_crud_request(crud_file_table[fd].object_id, CRUD_READ,  crud_file_table[fd].length, 0,0);
		char *temp= malloc(crud_file_table[fd].length);
		response = crud_client_operation(request,temp); 
		decryptResponse(response,&ID,&length, &result);	
		if(result!=0){
			free(temp);
			return -1;
	}
	
	if(crud_file_table[fd].position + count > crud_file_table[fd].length){
	
	        // Allocate new buffer of appropriate size
                char *newBuf = malloc(crud_file_table[fd].position + count);
                
		// Copy old memory into newBuf
	        memcpy(newBuf, temp, crud_file_table[fd].length);
                
		// Copy new bytes into newBuf at position 
                memcpy(&newBuf[crud_file_table[fd].position], buf, count);

		request = construct_crud_request(0, CRUD_CREATE,  crud_file_table[fd].position + count, 0,0);
		response = crud_client_operation(request,newBuf); 
		decryptResponse(response,&ID,&length, &result);	
	
		free(temp);
		free(newBuf);

		if(result!=0)
			return -1;
		
		CrudOID tempID =ID;
	
		
		request = construct_crud_request(crud_file_table[fd].object_id, CRUD_DELETE,  0, 0,0);
		response = crud_client_operation(request,NULL); 
		decryptResponse(response,&ID,&length, &result);	
 
		if(result!=0)
			return -1;
		
		// Update file information
            	crud_file_table[fd].object_id = tempID;
            	crud_file_table[fd].length = crud_file_table[fd].position + count;
            	crud_file_table[fd].position += count;
		
		
		return count;
	}

	else{
		memcpy(&temp[crud_file_table[fd].position], buf, count);

		request = construct_crud_request(crud_file_table[fd].object_id, CRUD_UPDATE,  crud_file_table[fd].length, 0,0);
		response = crud_client_operation(request,temp); 
		decryptResponse(response,&ID,&length, &result);	
		
		free(temp);
 
		if(result!=0)
			return -1;
		
		crud_file_table[fd].position += count;
		return count;
	}
}


}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_seek
// Description  : Seek to specific point in the file
//
// Inputs       : fd - the file descriptor for the file to seek
//                loc - offset from beginning of file to seek to
// Outputs      : 0 if successful or -1 if failure

int32_t crud_seek(int16_t fd, uint32_t loc) {

CrudOID ID;
int32_t length =0;
int result;
CrudRequest request;
CrudResponse response;

//
//local variables
//


	//check to see if the Crud system is initialized
	// if not then it initialies the system
	if(init == 0){
		request = construct_crud_request(0, CRUD_INIT, 0, 0,0);
		response = crud_client_operation(request,NULL); 
		decryptResponse(response,&ID,&length, &result);	
		if(result!=0)
			return -1;
		init = 1;
    	}

	//check to see if file is open
	if(crud_file_table[fd].open ==0)
		return -1;


	//update the file position
	crud_file_table[fd].position = loc;
	
	return 0;

}

// Module local methods

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crudIOUnitTest
// Description  : Perform a test of the CRUD IO implementation
//
// Inputs       : None
// Outputs      : 0 if successful or -1 if failure

int crudIOUnitTest(void) {

	// Local variables
	uint8_t ch;
	int16_t fh, i;
	int32_t cio_utest_length, cio_utest_position, count, bytes, expected;
	char *cio_utest_buffer, *tbuf;
	CRUD_UNIT_TEST_TYPE cmd;
	char lstr[1024];

	// Setup some operating buffers, zero out the mirrored file contents
	cio_utest_buffer = malloc(CRUD_MAX_OBJECT_SIZE);
	tbuf = malloc(CRUD_MAX_OBJECT_SIZE);
	memset(cio_utest_buffer, 0x0, CRUD_MAX_OBJECT_SIZE);
	cio_utest_length = 0;
	cio_utest_position = 0;

	// Format and mount the file system
	if (crud_format() || crud_mount()) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure on format or mount operation.");
		return(-1);
	}

	// Start by opening a file
	fh = crud_open("temp_file.txt");
	if (fh == -1) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure open operation.");
		return(-1);
	}

	// Now do a bunch of operations
	for (i=0; i<CRUD_IO_UNIT_TEST_ITERATIONS; i++) {

		// Pick a random command
		if (cio_utest_length == 0) {
			cmd = CIO_UNIT_TEST_WRITE;
		} else {
			cmd = getRandomValue(CIO_UNIT_TEST_READ, CIO_UNIT_TEST_SEEK);
		}

		// Execute the command
		switch (cmd) {

		case CIO_UNIT_TEST_READ: // read a random set of data
			count = getRandomValue(0, cio_utest_length);
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : read %d at position %d", bytes, cio_utest_position);
			bytes = crud_read(fh, tbuf, count);
			if (bytes == -1) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Read failure.");
				return(-1);
			}

			// Compare to what we expected
			if (cio_utest_position+count > cio_utest_length) {
				expected = cio_utest_length-cio_utest_position;
			} else {
				expected = count;
			}
			if (bytes != expected) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : short/long read of [%d!=%d]", bytes, expected);
				return(-1);
			}
			if ( (bytes > 0) && (memcmp(&cio_utest_buffer[cio_utest_position], tbuf, bytes)) ) {

				bufToString((unsigned char *)tbuf, bytes, (unsigned char *)lstr, 1024 );
				logMessage(LOG_INFO_LEVEL, "CIO_UTEST R: %s", lstr);
				bufToString((unsigned char *)&cio_utest_buffer[cio_utest_position], bytes, (unsigned char *)lstr, 1024 );
				logMessage(LOG_INFO_LEVEL, "CIO_UTEST U: %s", lstr);

				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : read data mismatch (%d)", bytes);
				return(-1);
			}
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : read %d match", bytes);


			// update the position pointer
			cio_utest_position += bytes;
			break;

		case CIO_UNIT_TEST_APPEND: // Append data onto the end of the file
			// Create random block, check to make sure that the write is not too large
			ch = getRandomValue(0, 0xff);
			count =  getRandomValue(1, CIO_UNIT_TEST_MAX_WRITE_SIZE);
			if (cio_utest_length+count >= CRUD_MAX_OBJECT_SIZE) {

				// Log, seek to end of file, create random value
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : append of %d bytes [%x]", count, ch);
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : seek to position %d", cio_utest_length);
				if (crud_seek(fh, cio_utest_length)) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : seek failed [%d].", cio_utest_length);
					return(-1);
				}
				cio_utest_position = cio_utest_length;
				memset(&cio_utest_buffer[cio_utest_position], ch, count);

				// Now write
				bytes = crud_write(fh, &cio_utest_buffer[cio_utest_position], count);
				if (bytes != count) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : append failed [%d].", count);
					return(-1);
				}
				cio_utest_length = cio_utest_position += bytes;
			}
			break;

		case CIO_UNIT_TEST_WRITE: // Write random block to the file
			ch = getRandomValue(0, 0xff);
			count =  getRandomValue(1, CIO_UNIT_TEST_MAX_WRITE_SIZE);
			// Check to make sure that the write is not too large
			if (cio_utest_length+count < CRUD_MAX_OBJECT_SIZE) {
				// Log the write, perform it
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : write of %d bytes [%x]", count, ch);
				memset(&cio_utest_buffer[cio_utest_position], ch, count);
				bytes = crud_write(fh, &cio_utest_buffer[cio_utest_position], count);
				if (bytes!=count) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : write failed [%d].", count);
					return(-1);
				}
				cio_utest_position += bytes;
				if (cio_utest_position > cio_utest_length) {
					cio_utest_length = cio_utest_position;
				}
			}
			break;

		case CIO_UNIT_TEST_SEEK:
			count = getRandomValue(0, cio_utest_length);
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : seek to position %d", count);
			if (crud_seek(fh, count)) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : seek failed [%d].", count);
				return(-1);
			}
			cio_utest_position = count;
			break;

		default: // This should never happen
			CMPSC_ASSERT0(0, "CRUD_IO_UNIT_TEST : illegal test command.");
			break;

		}

#if DEEP_DEBUG
		// VALIDATION STEP: ENSURE OUR LOCAL IS LIKE OBJECT STORE
		CrudRequest request;
		CrudResponse response;
		CrudOID oid;
		CRUD_REQUEST_TYPES req;
		uint32_t length;
		uint8_t res, flags;

		// Make a fake request to get file handle, then check it
		request = construct_crud_request(crud_file_table[0].object_id, CRUD_READ, CRUD_MAX_OBJECT_SIZE, CRUD_NULL_FLAG, 0);
		response = crud_client_operation(request, tbuf);
		if ((deconstruct_crud_request(response, &oid, &req, &length, &flags, &res) != 0) || (res != 0))  {
			logMessage(LOG_ERROR_LEVEL, "Read failure, bad CRUD response [%x]", response);
			return(-1);
		}
		if ( (cio_utest_length != length) || (memcmp(cio_utest_buffer, tbuf, length)) ) {
			logMessage(LOG_ERROR_LEVEL, "Buffer/Object cross validation failed [%x]", response);
			bufToString((unsigned char *)tbuf, length, (unsigned char *)lstr, 1024 );
			logMessage(LOG_INFO_LEVEL, "CIO_UTEST VR: %s", lstr);
			bufToString((unsigned char *)cio_utest_buffer, length, (unsigned char *)lstr, 1024 );
			logMessage(LOG_INFO_LEVEL, "CIO_UTEST VU: %s", lstr);
			return(-1);
		}

		// Print out the buffer
		bufToString((unsigned char *)cio_utest_buffer, cio_utest_length, (unsigned char *)lstr, 1024 );
		logMessage(LOG_INFO_LEVEL, "CIO_UTEST: %s", lstr);
#endif

	}

	// Close the files and cleanup buffers, assert on failure
	if (crud_close(fh)) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure read comparison block.", fh);
		return(-1);
	}
	free(cio_utest_buffer);
	free(tbuf);

	// Format and mount the file system
	if (crud_unmount()) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure on unmount operation.");
		return(-1);
	}

	// Return successfully
	return(0);
}



























