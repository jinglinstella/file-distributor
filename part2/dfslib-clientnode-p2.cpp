#include <regex>
#include <mutex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <csignal>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>
#include <utime.h>

#include "src/dfs-utils.h"
#include "src/dfslibx-clientnode-p2.h"
#include "dfslib-shared-p2.h"
#include "dfslib-clientnode-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"
#include <google/protobuf/util/time_util.h>

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;

using google::protobuf::RepeatedPtrField;
using google::protobuf::util::TimeUtil;

using std::chrono::system_clock;
using std::chrono::milliseconds;

using namespace dfs_service;
using namespace std;

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using to indicate
// a file request and a listing of files from the server.
//
using FileRequestType = dfs_service::File;
using FileListResponseType = dfs_service::Files;

DFSClientNodeP2::DFSClientNodeP2() : DFSClientNode() {}
DFSClientNodeP2::~DFSClientNodeP2() {}

grpc::StatusCode DFSClientNodeP2::RequestWriteAccess(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to obtain a write lock here when trying to store a file.
    // This method should request a write lock for the given file at the server,
    // so that the current client becomes the sole creator/writer. If the server
    // responds with a RESOURCE_EXHAUSTED response, the client should cancel
    // the current file storage
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    ClientContext context;
    context.AddMetadata("client_id", ClientId());

    File request;
    request.set_name(filename);

    WriteLock response;

    //pass the specification "AcquireWriteLock" to server
    //server will run the actual function
    Status status = service_stub->AcquireWriteLock(&context, request, &response);
    if (!status.ok()) {
        return status.error_code();
    } else{
        return StatusCode::OK;
    }
    
}

grpc::StatusCode DFSClientNodeP2::Store(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // stored is the same on the server (i.e. the ALREADY_EXISTS gRPC response).
    //
    // You will also need to add a request for a write lock before attempting to store.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::ALREADY_EXISTS - if the local cached file has not changed from the server version
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    const string& file_path = WrapPath(filename);

    struct stat fs;
    stat(file_path.c_str(), &fs);

    StatusCode writeLockStatus = this->RequestWriteAccess(filename);
    if (writeLockStatus != StatusCode::OK) {
        return StatusCode::RESOURCE_EXHAUSTED;
    }

    ClientContext context;

    context.AddMetadata("client_id", ClientId());
    context.AddMetadata("file_name", filename);
    context.AddMetadata("checksum", to_string(dfs_file_checksum(file_path, &crc_table)));
    context.AddMetadata("mtime", to_string(static_cast<long>(fs.st_mtime)));

    ServerResponse response;

    //get filesize based on fs which comes from file path
    int fileSize = fs.st_size;

    unique_ptr<ClientWriter<FileStream>> server_response = service_stub->StoreFile(&context, &response);
    ifstream ifs(file_path);
    FileStream chunk;
    int bytes_sent = 0;

    while(bytes_sent < fileSize){
          char buffer[10240];
          int bytes_remain = fileSize - bytes_sent;
          if (bytes_remain > 10240){
              bytes_remain = 10240;
          }
          ifs.read(buffer, bytes_remain);
          chunk.set_contents(buffer, bytes_remain);
          server_response->Write(chunk);
        bytes_sent += bytes_remain;
    }

    server_response->WritesDone();
    Status status = server_response->Finish();
    if (!status.ok() && status.error_code() == StatusCode::INTERNAL) {
        return StatusCode::CANCELLED;
    }
    return status.error_code();

}


grpc::StatusCode DFSClientNodeP2::Fetch(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // fetched is the same on the client (i.e. the files do not differ
    // between the client and server and a fetch would be unnecessary.
    //
    // The StatusCode server_response should be:
    //
    // OK - if all went well
    // DEADLINE_EXCEEDED - if the deadline timeout occurs
    // NOT_FOUND - if the file cannot be found on the server
    // ALREADY_EXISTS - if the local cached file has not changed from the server version
    // CANCELLED otherwise
    //
    // Hint: You may want to match the mtime on local files to the server's mtime
    //
    const string& file_path = WrapPath(filename);

    ClientContext context;
    struct stat fs;
    if (stat(file_path.c_str(), &fs) == 0){
        context.AddMetadata("mtime", to_string(static_cast<long>(fs.st_mtime)));
    }

    context.AddMetadata("checksum", to_string(dfs_file_checksum(file_path, &crc_table)));

    File request;
    request.set_name(filename);

    unique_ptr<ClientReader<FileStream>> server_response = service_stub->FetchFile(&context, request);
    ofstream ofs;
    FileStream chunk;

        while (server_response->Read(&chunk)) {
            if (!ofs.is_open()){
                ofs.open(file_path, ios::trunc);
            }
            ofs << chunk.contents();

        }
        ofs.close();

    Status status = server_response->Finish();
    if (!status.ok() && status.error_code() == StatusCode::INTERNAL) {
        return StatusCode::CANCELLED;

    }
    return status.error_code();

}

grpc::StatusCode DFSClientNodeP2::Delete(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You will also need to add a request for a write lock before attempting to delete.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    StatusCode writeLockStatus = this->RequestWriteAccess(filename);
    if (writeLockStatus != StatusCode::OK) {
        return StatusCode::RESOURCE_EXHAUSTED;
    }

    ClientContext context;
    context.AddMetadata("client_id", ClientId());

    File request;
    request.set_name(filename);

    ServerResponse response;

    Status status = service_stub->DeleteFile(&context, request, &response);

    return status.error_code();


}

grpc::StatusCode DFSClientNodeP2::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list files here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // listing details that would be useful to your solution to the list response.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
    //
    //

    ClientContext context;

    VoidArg request;
    Files response;

    Status status = service_stub->ListFiles(&context, request, &response);


    for (const FileStatus& fs : response.file()) {
        //add file to file map
        file_map->insert(pair<string,int>(fs.name(), TimeUtil::TimestampToSeconds(fs.modified())));
    }
    return status.error_code();

}

grpc::StatusCode DFSClientNodeP2::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // status details that would be useful to your solution.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //
    ClientContext context;

    File request;
    request.set_name(filename);
    FileStatus response;

    Status status = service_stub->GetStatus(&context, request, &response);
    if (!status.ok() && status.error_code() == StatusCode::INTERNAL) {
        return StatusCode::CANCELLED;
    }

    file_status = &response;

    return status.error_code();

}

void DFSClientNodeP2::InotifyWatcherCallback(std::function<void()> callback) {

    //
    // STUDENT INSTRUCTION:
    //
    // This method gets called each time inotify signals a change
    // to a file on the file system. That is every time a file is
    // modified or created.
    //
    // You may want to consider how this section will affect
    // concurrent actions between the inotify watcher and the
    // asynchronous callbacks associated with the server.
    //
    // The callback method shown must be called here, but you may surround it with
    // whatever structures you feel are necessary to ensure proper coordination
    // between the async and watcher threads.
    //
    // Hint: how can you prevent race conditions between this thread and
    // the async thread when a file event has been signaled?
    //

    listMutex.lock();

    callback();

    listMutex.unlock();
}

//
// STUDENT INSTRUCTION:
//
// This method handles the gRPC asynchronous callbacks from the server.
// We've provided the base structure for you, but you should review
// the hints provided in the STUDENT INSTRUCTION sections below
// in order to complete this method.
//
void DFSClientNodeP2::HandleCallbackList() {

    void* tag;

    bool ok = false;

    //
    // STUDENT INSTRUCTION:
    //
    // Add your file list synchronization code here.
    //
    // When the server responds to an asynchronous request for the CallbackList,
    // this method is called. You should then synchronize the
    // files between the server and the client based on the goals
    // described in the readme.
    //
    // In addition to synchronizing the files, you'll also need to ensure
    // that the async thread and the file watcher thread are cooperating. These
    // two threads could easily get into a race condition where both are trying
    // to write or fetch over top of each other. So, you'll need to determine
    // what type of locking/guarding is necessary to ensure the threads are
    // properly coordinated.
    //

    // Block until the next result is available in the completion queue.
    while (completion_queue.Next(&tag, &ok)) {
        {
            //
            // STUDENT INSTRUCTION:
            //
            // Consider adding a critical section or RAII style lock here
            //

            // The tag is the memory location of the call_data object
            // received completion_queue callback
            AsyncClientData<FileListResponseType> *call_data = static_cast<AsyncClientData<FileListResponseType> *>(tag);

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            // GPR_ASSERT(ok);

            if (ok && call_data->status.ok()) {

                //
                // STUDENT INSTRUCTION:
                //
                // Add your handling of the asynchronous event calls here.
                // For example, based on the file listing returned from the server,
                // how should the client respond to this updated information?
                // Should it retrieve an updated version of the file?
                // Send an update to the server?
                // Do nothing?
                //

                listMutex.lock();

                //parse the modifications sent from server
                for (const FileStatus& remote_status : call_data->reply.file()) {
                    const string& file_path = WrapPath(remote_status.name());

                    FileStatus local_status;
                    StatusCode status;

                    struct stat fs;
                    int local = stat(file_path.c_str(), &fs);

                    if (local != 0) {
                        //doesn't exist locally, fetching
                        this->Fetch(remote_status.name());

                    }

                    if(local == 0 && remote_status.modified() > local_status.modified()) {
                    //local file is out of date
                        status = this->Fetch(remote_status.name());
                        if (status == StatusCode::ALREADY_EXISTS) {

                            struct utimbuf ub;
                            ub.modtime = TimeUtil::TimestampToTimeT(remote_status.modified()); ;
                            utime(file_path.c_str(), &ub);

                        }
                    // server out of date
                    } else if (local == 0 && local_status.modified() > remote_status.modified()) {

                        this->Store(remote_status.name());

                    }
                }

                listMutex.unlock();

            } else {
                dfs_log(LL_ERROR) << "Status was not ok. Will try again in " << DFS_RESET_TIMEOUT << " milliseconds.";
                dfs_log(LL_ERROR) << call_data->status.error_message();
                std::this_thread::sleep_for(std::chrono::milliseconds(DFS_RESET_TIMEOUT));
            }


            // Once we're complete, deallocate the call_data object.
            delete call_data;

            //
            // STUDENT INSTRUCTION:
            //
            // Add any additional syncing/locking mechanisms you may need here

        }


        // Start the process over and wait for the next callback response
        dfs_log(LL_DEBUG3) << "Calling InitCallbackList";
        InitCallbackList();

    }
}

/**
 * This method will start the callback request to the server, requesting
 * an update whenever the server sees that files have been modified.
 *
 * We're making use of a template function here, so that we can keep some
 * of the more intricate workings of the async process out of the way, and
 * give you a chance to focus more on the project's requirements.
 */
void DFSClientNodeP2::InitCallbackList() {
    CallbackList<FileRequestType, FileListResponseType>();
}



