#include <map>
#include <mutex>
#include <shared_mutex>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <errno.h>
#include <iostream>
#include <fstream>
#include <getopt.h>
#include <dirent.h>
#include <sys/stat.h>
#include <grpcpp/grpcpp.h>
#include <utime.h>

#include "proto-src/dfs-service.grpc.pb.h"
#include "src/dfslibx-call-data.h"
#include "src/dfslibx-service-runner.h"
#include "dfslib-shared-p2.h"
#include "dfslib-servernode-p2.h"
#include <google/protobuf/util/time_util.h>

using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;
using grpc::string_ref;

using namespace dfs_service;

using google::protobuf::util::TimeUtil;
using google::protobuf::Timestamp;
using google::protobuf::uint64;

using namespace std;

//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using in your `dfs-service.proto` file
// to indicate a file request and a listing of files from the server
//
using FileRequestType = dfs_service::File;
using FileListResponseType = dfs_service::Files;

using FileName = string;
using ClientId = string;

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// As with Part 1, the DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in your `dfs-service.proto` file.
//
// You may start with your Part 1 implementations of each service method.
//
// Elements to consider for Part 2:
//
// - How will you implement the write lock at the server level?
// - How will you keep track of which client has a write lock for a file?
//      - Note that we've provided a preset client_id in DFSClientNode that generates
//        a client id for you. You can pass that to the server to identify the current client.
// - How will you release the write lock?
// - How will you handle a store request for a client that doesn't have a write lock?
// - When matching files to determine similarity, you should use the `file_checksum` method we've provided.
//      - Both the client and server have a pre-made `crc_table` variable to speed things up.
//      - Use the `file_checksum` method to compare two files, similar to the following:
//
//          std::uint32_t server_crc = dfs_file_checksum(filepath, &this->crc_table);
//
//      - Hint: as the crc checksum is a simple integer, you can pass it around inside your message types.
//
class DFSServiceImpl final :
        public DFSService::WithAsyncMethod_CallbackList<DFSService::Service>,
        public DFSCallDataManager<FileRequestType , FileListResponseType> {

private:

    /** The runner service used to start the service and manage asynchronicity **/
    DFSServiceRunner<FileRequestType, FileListResponseType> runner;

    /** The mount path for the server **/
    std::string mount_path;

    /** Mutex for managing the queue requests **/
    std::mutex queue_mutex;

    /** The vector of queued tags used to manage asynchronous requests **/
    std::vector<QueueRequest<FileRequestType, FileListResponseType>> queued_tags;


    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }

    /** CRC Table kept in memory for faster calculations **/
    CRC::Table<std::uint32_t, 32> crc_table;

    shared_timed_mutex clientid_mutex;
    map<FileName, ClientId> filename_clientid_map;

    shared_timed_mutex filename_mutex;
    map<FileName, unique_ptr<shared_timed_mutex>> filename_mutex_map;

    shared_timed_mutex list_mutex;


public:

    DFSServiceImpl(const std::string& mount_path, const std::string& server_address, int num_async_threads):
            mount_path(mount_path), crc_table(CRC::CRC_32()) {

        this->runner.SetService(this);
        this->runner.SetAddress(server_address);
        this->runner.SetNumThreads(num_async_threads);
        this->runner.SetQueuedRequestsCallback([&]{ this->ProcessQueuedRequests(); });

        // populate existing files in inProgressFileNameToMutex
        DIR *dir;
        dir = opendir(mount_path.c_str());

        // iterate the entire directory, return info about directory entries
        struct dirent *itr;

        //loop through the directory
        while ((itr = readdir(dir)) != NULL) {
            struct stat path_info;
            //get file name
            string path = WrapPath(itr->d_name);
            stat(path.c_str(), &path_info);
            // if dir item is a file

            if(stat(path.c_str(), &path_info) == 0 && path_info.st_mode & S_IFREG){
                //give each file a mutex for read/write
                filename_mutex_map[itr->d_name] = make_unique<shared_timed_mutex>();
            }

        }
        closedir(dir);
    }

    ~DFSServiceImpl() {
        this->runner.Shutdown();
    }

    void Run() {
        this->runner.Run();
    }

    /**
     * Request callback for asynchronous requests
     *
     * This method is called by the DFSCallData class during
     * an asynchronous request call from the client.
     *
     * Students should not need to adjust this.
     *
     * @param context
     * @param request
     * @param response
     * @param cq
     * @param tag
     */
    void RequestCallback(grpc::ServerContext* context,
                         FileRequestType* request,
                         grpc::ServerAsyncResponseWriter<FileListResponseType>* response,
                         grpc::ServerCompletionQueue* cq,
                         void* tag) {

        std::lock_guard<std::mutex> lock(queue_mutex);
        this->queued_tags.emplace_back(context, request, response, cq, tag);

    }

    /**
     * Process a callback request
     *
     * This method is called by the DFSCallData class when
     * a requested callback can be processed. You should use this method
     * to manage the CallbackList RPC call and respond as needed.
     *
     * See the STUDENT INSTRUCTION for more details.
     *
     * @param context
     * @param request
     * @param response
     */
    void ProcessCallback(ServerContext* context, FileRequestType* request, FileListResponseType* response) {

        //
        // STUDENT INSTRUCTION:
        //
        // You should add your code here to respond to any CallbackList requests from a client.
        // This function is called each time an asynchronous request is made from the client.
        //
        // The client should receive a list of files or modifications that represent the changes this service
        // is aware of. The client will then need to make the appropriate calls based on those changes.
        //


        this->CallbackList(context, request, response);


    }

    /**
     * Processes the queued requests in the queue thread
     * no change for this part
     */
    void ProcessQueuedRequests() {
        while(true) {

            //
            // STUDENT INSTRUCTION:
            //
            // You should add any synchronization mechanisms you may need here in
            // addition to the queue management. For example, modified files checks.
            //
            // Note: you will need to leave the basic queue structure as-is, but you
            // may add any additional code you feel is necessary.
            // no need to change


            // Guarded section for queue
            {
                dfs_log(LL_DEBUG2) << "Waiting for queue guard";
                std::lock_guard<std::mutex> lock(queue_mutex);


                for(QueueRequest<FileRequestType, FileListResponseType>& queue_request : this->queued_tags) {
                    this->RequestCallbackList(queue_request.context, queue_request.request,
                                              queue_request.response, queue_request.cq, queue_request.cq, queue_request.tag);
                    queue_request.finished = true;
                }

                // any finished tags first
                this->queued_tags.erase(std::remove_if(
                        this->queued_tags.begin(),
                        this->queued_tags.end(),
                        [](QueueRequest<FileRequestType, FileListResponseType>& queue_request) { return queue_request.finished; }
                ), this->queued_tags.end());

            }
        }
    }

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // the implementations of your rpc protocol methods.
    //

    Status AcquireWriteLock(
            ServerContext* context,
            const File* request,
            WriteLock* response
    ) override {
        //grab request from client
        const multimap<string_ref, string_ref>& metadata = context->client_metadata();

        //find the pointer to "client_id : client id"
        auto clientid_itr = metadata.find("client_id");
        // File name is missing

        //extract client id from the pointer
        auto clientid = string(clientid_itr->second.begin(), clientid_itr->second.end());

        //the code below is the critical section that needs protection
        clientid_mutex.lock();

        //find the pointer to [filename : client id with lock]
        //the map "filename_clientid_map" stores all filename that is held by a clientid
        //as long as it's in the map it holds a mutex
        auto clientid_lock_itr = filename_clientid_map.find(request->name());

        string clientid_lock = clientid_lock_itr->second;

        //if this filename is not held by any client
        if(clientid_lock_itr == filename_clientid_map.end()){

            //give this filename to the client requested it
            filename_clientid_map[request->name()] = clientid;
            clientid_mutex.unlock();

            // add mutex if it doesn't exist
            filename_mutex.lock();
            if (filename_mutex_map.find(request->name()) == filename_mutex_map.end()){
                filename_mutex_map[request->name()] = make_unique<shared_timed_mutex>();
            }
            filename_mutex.unlock();
            return Status::OK;

        }

        else
        {
            if (clientid_lock.compare(clientid) == 0)
            {
                clientid_mutex.unlock();
                //client already has a lock
                return Status::OK;
            }


            else
            {
                clientid_mutex.unlock();
                return Status(StatusCode::RESOURCE_EXHAUSTED, "lock occupied by other client");

            }
        }


    }

    Status StoreFile(
            ServerContext* context,
            ServerReader<FileStream>* reader,
            ServerResponse* response
    ) override {
        const multimap<string_ref, string_ref>& metadata = context->client_metadata();

        auto clientid_itr = metadata.find("client_id");
        auto filename_itr = metadata.find("file_name");
        auto mtime_itr = metadata.find("mtime");

        auto clientid = string(clientid_itr->second.begin(), clientid_itr->second.end());
        auto file_name = string(filename_itr->second.begin(), filename_itr->second.end());
        long mtime = stol(string(mtime_itr->second.begin(), mtime_itr->second.end()));

        string file_path = WrapPath(file_name);

        clientid_mutex.lock_shared();
        auto clientid_lock_itr = filename_clientid_map.find(file_name);
        string clientid_lock = clientid_lock_itr->second;
        clientid_mutex.unlock_shared();

        filename_mutex.lock_shared();
        //iterator of the map filename_mutex_map
        auto file_write_lock = filename_mutex_map.find(file_name);
        //get unique_ptr<shared_timed_mutex>
        shared_timed_mutex* access_mutex = file_write_lock->second.get();
        filename_mutex.unlock_shared();

        list_mutex.lock();
        access_mutex->lock();

        auto checksum_itr = metadata.find("checksum");
        unsigned long client_checksum = stoul(string(checksum_itr->second.begin(), checksum_itr->second.end()));
        unsigned long server_checksum = dfs_file_checksum(file_path, &crc_table);

        if(client_checksum == server_checksum)
        {
            struct stat fs;
            if(stat(file_path.c_str(), &fs) == 0 && mtime > fs.st_mtime)
            {
                //server out of date, but content same
                struct utimbuf ub;
                ub.modtime = mtime;
                utime(file_path.c_str(), &ub);

            }

            clientid_mutex.lock();
            filename_clientid_map.erase(file_name);
            clientid_mutex.unlock();

            access_mutex->unlock();
            list_mutex.unlock();

            return Status(StatusCode::ALREADY_EXISTS, "client and server have same content");
        }


        FileStream chunk;
        ofstream ofs;

        while (reader->Read(&chunk)) {
            if (!ofs.is_open()) {
                ofs.open(file_path, ios::trunc);
            }
            ofs << chunk.contents();
        }
        ofs.close();

        clientid_mutex.lock();
        filename_clientid_map.erase(file_name);
        clientid_mutex.unlock();

        access_mutex->unlock();
        list_mutex.unlock();

        response->set_file_name(file_name);
        FileStatus fs;
        Timestamp* modified = new Timestamp(fs.modified());
        response->set_allocated_modified(modified);
        return Status::OK;
    }

    Status FetchFile(
            ServerContext* context,
            const File* request,
            ServerWriter<FileStream>* writer
    ) override {
        string file_path = WrapPath(request->name());
        const multimap<string_ref, string_ref>& metadata = context->client_metadata();

        string file_name = request->name();
        filename_mutex.lock();
        if (filename_mutex_map.find(file_name) == filename_mutex_map.end()){
            filename_mutex_map[file_name] = make_unique<shared_timed_mutex>();
        }
        filename_mutex.unlock();

        filename_mutex.lock_shared();
        //iterator of the map filename_mutex_map
        auto file_write_itr = filename_mutex_map.find(request->name());
        //get unique_ptr<shared_timed_mutex>
        shared_timed_mutex* access_mutex = file_write_itr->second.get();
        filename_mutex.unlock_shared();

        access_mutex->lock();
        struct stat fs;
        if (stat(file_path.c_str(), &fs) != 0){
            access_mutex->unlock();
            return Status(StatusCode::NOT_FOUND, "file not found");
        }

        auto checksum_itr = metadata.find("checksum");

        unsigned long client_checksum = stoul(string(checksum_itr->second.begin(), checksum_itr->second.end()));
        unsigned long server_checksum = dfs_file_checksum(file_path, &crc_table);


        if (client_checksum == server_checksum){

            const multimap<string_ref, string_ref>& metadata = context->client_metadata();
            auto mtime_itr = metadata.find("mtime");

            long mtime = stol(string(mtime_itr->second.begin(), mtime_itr->second.end()));

            if (mtime > fs.st_mtime){
                //client time > server time, server outdated, but contents are same
                struct utimbuf ub;
                ub.modtime = mtime;
                ub.actime = mtime;
                utime(file_path.c_str(), &ub);

            }

            access_mutex->unlock();

            return Status(StatusCode::ALREADY_EXISTS, "client and server have same content");
        }

        access_mutex->unlock();
        access_mutex->lock_shared();

        int file_size = fs.st_size;

        ifstream ifs(file_path);
        FileStream chunk;

        int bytes_sent = 0;
        int bytes_remain = 0;

        while(bytes_sent < file_size)
        {
            bytes_remain = file_size - bytes_sent;
            if(bytes_remain > 10240){
                bytes_remain = 10240;
            }
            char buffer[10240];
            ifs.read(buffer, bytes_remain);
            chunk.set_contents(buffer, bytes_remain);
            writer->Write(chunk);
            bytes_sent = bytes_sent + bytes_remain;
        }
        ifs.close();
        access_mutex->unlock_shared();

        return Status::OK;
    }

    Status DeleteFile(
            ServerContext* context,
            const File* request,
            ServerResponse* response
    ) override {
        const string& file_path = WrapPath(request->name());

        const multimap<string_ref, string_ref>& metadata = context->client_metadata();

        auto clientit_itr = metadata.find("client_id");

        auto clientid = string(clientit_itr->second.begin(), clientit_itr->second.end());

        //string fileName = request->name();
        filename_mutex.lock();
        if (filename_mutex_map.find(request->name()) == filename_mutex_map.end()){
            filename_mutex_map[request->name()] = make_unique<shared_timed_mutex>();
        }
        filename_mutex.unlock();

        filename_mutex.lock_shared();
        //iterator of the map filename_mutex_map
        auto access_mutex_itr = filename_mutex_map.find(request->name());
        //get unique_ptr<shared_timed_mutex>
        shared_timed_mutex* access_mutex = access_mutex_itr->second.get();
        filename_mutex.unlock_shared();


        clientid_mutex.lock_shared();

        auto client_lock_itr = filename_clientid_map.find(request->name());
        string clientid_lock = client_lock_itr->second;

        if (clientid_lock.compare(clientid) != 0)
        {
            clientid_mutex.unlock_shared();
            return Status(StatusCode::RESOURCE_EXHAUSTED, "lock occupied by other client");
        }
        clientid_mutex.unlock_shared();

        list_mutex.lock();
        access_mutex->lock();

        FileStatus fs;
        struct stat result;
        if (stat(file_path.c_str(), &result) != 0){
            return Status(StatusCode::NOT_FOUND, "file not found");
        }

        remove(file_path.c_str());

        clientid_mutex.lock();
        filename_clientid_map.erase(request->name());
        clientid_mutex.unlock();

        access_mutex->unlock();
        list_mutex.unlock();

        response->set_file_name(request->name());
        Timestamp* modified = new Timestamp(fs.modified());
        response->set_allocated_modified(modified);

        return Status::OK;
    }

    Status ListFiles(
            ServerContext* context,
            const VoidArg* request,
            Files* response
    ) override {
        list_mutex.lock_shared();
        DIR *dir;
        dir = opendir(mount_path.c_str());

        struct dirent *itr;
        while ((itr = readdir(dir)) != NULL) {

            struct stat path_info;

            string path = WrapPath(itr->d_name);

            if(stat(path.c_str(), &path_info) == 0){
                if(path_info.st_mode & S_IFREG){

                    //found a file
                    FileStatus* resp = response->add_file();
                    FileStatus status;

                    struct stat result;
                    if(stat(path.c_str(), &result) != 0){
                        return Status(StatusCode::NOT_FOUND, "not found");
                    }


                    resp->set_name(itr->d_name);
                    Timestamp* modified = new Timestamp(status.modified());
                    Timestamp* created = new Timestamp(status.created());
                    resp->set_allocated_created(created);
                    resp->set_allocated_modified(modified);
                    resp->set_size(status.size());

                }

            }

        }
        closedir(dir);
        list_mutex.unlock_shared();

        return Status::OK;
    }

    Status GetStatus(
            ServerContext* context,
            const File* request,
            FileStatus* response
    ) override {
        if (context->IsCancelled()){
            return Status(StatusCode::DEADLINE_EXCEEDED, "deadline exceeded");
        }

        string file_path = WrapPath(request->name());

        //string fileName = request->name();
        filename_mutex.lock();
        if (filename_mutex_map.find(request->name()) == filename_mutex_map.end()){
            filename_mutex_map[request->name()] = make_unique<shared_timed_mutex>();
        }
        filename_mutex.unlock();

        filename_mutex.lock_shared();
        //iterator of the map filename_mutex_map
        auto access_mutex_itr = filename_mutex_map.find(request->name());
        //get unique_ptr<shared_timed_mutex>
        shared_timed_mutex* access_mutex = access_mutex_itr->second.get();
        filename_mutex.unlock_shared();

        access_mutex->lock_shared();
        /* Get FileStatus of file */

        struct stat result;
        if (stat(file_path.c_str(), &result) != 0){
            access_mutex->unlock_shared();
            return Status(StatusCode::NOT_FOUND, "file not found");
        }

        access_mutex->unlock_shared();

        return Status::OK;
    }

    Status CallbackList(
            ServerContext* context,
            const File* request,
            Files* response
    ) override {
        VoidArg req;
        return this->ListFiles(context, &req, response);
    }



};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly
// to add additional startup/shutdown routines inside, but be aware that
// the basic structure should stay the same as the testing environment
// will be expected this structure.
//
/**
 * The main server node constructor
 *
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
                             const std::string &mount_path,
                             int num_async_threads,
                             std::function<void()> callback) :
        server_address(server_address),
        mount_path(mount_path),
        num_async_threads(num_async_threads),
        grader_callback(callback) {}
/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
}

/**
 * Start the DFSServerNode server
 */
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path, this->server_address, this->num_async_threads);


    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    service.Run();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional definitions here
//

