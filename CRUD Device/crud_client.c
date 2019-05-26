////////////////////////////////////////////////////////////////////////////////
//
//  File          : crud_client.c
//  Description   : This is the client side of the CRUD communication protocol.
//
//   Author       : Patrick McDaniel
//  Last Modified : Thu Oct 30 06:59:59 EDT 2014
//

// Include Files

// Project Include Files
#include <crud_network.h>
#include <cmpsc311_log.h>
#include <cmpsc311_util.h>

// Global variables
int            crud_network_shutdown = 0; // Flag indicating shutdown
unsigned char *crud_network_address = NULL; // Address of CRUD server 
unsigned short crud_network_port = 0; // Port of CRUD server

//
// Functions

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_client_operation
// Description  : This the client operation that sends a request to the CRUD
//                server.   It will:
//
//                1) if INIT make a connection to the server
//                2) send any request to the server, returning results
//                3) if CLOSE, will close the connection
//
// Inputs       : op - the request opcode for the command
//                buf - the block to be read/written from (READ/WRITE)
// Outputs      : the response structure encoded as needed

#include <crud_network.h>
#include <cmpsc311_log.h>
#include <cmpsc311_util.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

// Global variables
//int            crud_network_shutdown = 0; // Flag indicating shutdown
//unsigned char *crud_network_address = NULL; // Address of CRUD server 
//unsigned short crud_network_port = 0; // Port of CRUD server
int            socket_fd = -1; // socket file descriptor

//
// Functions

int crud_send(CrudRequest request, void *buf);
CrudResponse crud_receive(void *buf);

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_client_operation
// Description  : This the client operation that sends a request to the CRUD
//                server.   It will:
//
//                1) if INIT make a connection to the server
//                2) send any request to the server, returning results
//                3) if CLOSE, will close the connection
//
// Inputs       : op - the request opcode for the command
//                buf - the block to be read/written from (READ/WRITE)
// Outputs      : the response structure encoded as needed

CrudResponse crud_client_operation(CrudRequest op, void *buf) {
    // Declare variables
    CrudResponse response;
    uint8_t request;

    // get the request type
    request = (op >> 28) & 0xf;

    // if CRUD_INIT then make a connection to the server 
    if (request == CRUD_INIT)
    {
        // Create socket
        socket_fd = socket(PF_INET, SOCK_STREAM, 0);
        if (socket_fd == -1)
        {
            printf("Error on socket creation\n");
            return(-1);
        }

        // Specify address to connect to
        struct sockaddr_in caddr;
        caddr.sin_family = AF_INET;
        caddr.sin_port = htons(CRUD_DEFAULT_PORT);
        if (inet_aton(CRUD_DEFAULT_IP, &caddr.sin_addr) == 0)
        {
            return(-1);
        }

        // Connect
        if (connect(socket_fd, (const struct sockaddr *)&caddr,
                    sizeof(struct sockaddr)) == -1)
        {
            printf("Error connecting to server\n");
            return(-1);
        }
    }

    // Send request to server
    if (crud_send(op, buf) != 0)
        return -1;

    // Receive response
    response = crud_receive(buf);

    // if CRUD_CLOSE, close the connection
    if (request == CRUD_CLOSE) 
    {
        close(socket_fd);
        socket_fd = -1;
    }

    return response;
	
}


////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_send
// Description  : This is the function that sends the client CrudRequest to the
//                  server (and buffer if necessary).
//
// Inputs       : request - the request opcode for the command
//                buf - the block to be read/written from (READ/WRITE)
// Outputs      : 0 if successful, -1 if error 

int crud_send(CrudRequest request, void *buf)
{
    // Declare variables
    int requestLen = sizeof(CrudRequest);
    int reqAmtWritten;
    CrudRequest *requestOrder = malloc(requestLen);
    int req = (request >> 28) & 0xf;
    int bufLength = (request >> 4) & 0xffffff;
    int bufAmtWritten;

    // Convert request value to network byte order 
    *requestOrder = htonll64(request);

    // Send converted request value, make sure all bytes are sent
    reqAmtWritten = write(socket_fd, requestOrder, requestLen); 
    while (reqAmtWritten < requestLen)
    {
        reqAmtWritten += write(socket_fd, (char *)requestOrder+reqAmtWritten,
                requestLen - reqAmtWritten);
    }
    free(requestOrder);

    // Check if you need to send buffer as well
    if (req == CRUD_CREATE || req == CRUD_UPDATE)
    {
        bufAmtWritten = write(socket_fd, buf, bufLength);
        while (bufAmtWritten < bufLength)
        {
            bufAmtWritten += write(socket_fd, ((char *)buf)+bufAmtWritten, bufLength - bufAmtWritten);
        }
    }

    return 0;
}


////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_receive
// Description  : This is the function that receives the server response (and
//                  buffer if necessary).
//
// Inputs       : buf - the block to be read/written from (READ/WRITE)
// Outputs      : server CrudResponse

CrudResponse crud_receive(void *buf)
{
    // Declare variables
    CrudResponse responseOrder;
    int reponseLen = sizeof(CrudResponse);
    int responseAmtRead;
    CrudResponse *response = malloc(reponseLen);
    int bufLen;
    int bufAmtRead;
    int responseRequest;

    // Receive response value
    responseAmtRead = read(socket_fd, response, reponseLen);
    while (responseAmtRead < reponseLen)
    {
        responseAmtRead += read(socket_fd, (char *)response +responseAmtRead, reponseLen - responseAmtRead);
    }

    // Convert received value into host byte order
    responseOrder = ntohll64(*response);
    free(response);

    // Extract request type and length from converted response
    responseRequest = (responseOrder >> 28) & 0xf;
    bufLen = (responseOrder >> 4) & 0xffffff;

    // Check if you need to receive buffer
    if (responseRequest == CRUD_READ)
    {
        bufAmtRead = read(socket_fd, buf, bufLen);
        while (bufAmtRead < bufLen)
        {
            bufAmtRead += read(socket_fd, ((char *)buf)+bufAmtRead, bufLen - bufAmtRead);
        }
    }


    return responseOrder;
}
