#include <map>
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

#include "src/dfs-utils.h"
#include "dfslib-shared-p1.h"
#include "dfslib-servernode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"
#include <google/protobuf/util/time_util.h>

using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;
using grpc::string_ref;

using dfs_service::DFSService;
using dfs_service::FileStream;
using dfs_service::ServerResponse;
using dfs_service::File;
using dfs_service::Files;
using dfs_service::VoidArg;
using dfs_service::FileStatus;

using google::protobuf::util::TimeUtil;
using google::protobuf::Timestamp;
using google::protobuf::uint64;

using namespace std;


//
// STUDENT INSTRUCTION:
//
// DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in the `dfs-service.proto` file.
//
// You should add your definition overrides here for the specific
// methods that you created for your GRPC service protocol. The
// gRPC tutorial described in the readme is a good place to get started
// when trying to understand how to implement this class.
//
// The method signatures generated can be found in `proto-src/dfs-service.grpc.pb.h` file.
//
// Look for the following section:
//
//      class Service : public Service {
//
// The methods returning grpc::Status are the methods you'll want to override.
//
// In C++, you'll want to use the `override` directive as well. For example,
// if you have a service method named MyMethod that takes a MyMessageType
// and a ServerWriter, you'll want to override it similar to the following:
//
//      Status MyMethod(ServerContext* context,
//                      const MyMessageType* request,
//                      ServerWriter<MySegmentType> *writer) override {
//
//          /** code implementation here **/
//      }
//
class DFSServiceImpl final : public DFSService::Service {

private:

    /** The mount path for the server **/
    std::string mount_path;

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }


public:

    DFSServiceImpl(const std::string &mount_path): mount_path(mount_path) {
    }

    ~DFSServiceImpl() {}

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // implementations of your protocol service methods
    //

    Status StoreFile(
        ServerContext* context,
        ServerReader<FileStream>* reader,
        ServerResponse* response
    ) override {
        const multimap<string_ref, string_ref>& metadata = context->client_metadata();
        auto iterator = metadata.find("file_name");

        auto fileName = string(iterator->second.begin(), iterator->second.end());

        const string& file_path = WrapPath(fileName);


        FileStream file_chunk; //msg from client
        ofstream ofs;

        while (reader->Read(&file_chunk)) {
            if (!ofs.is_open()) {
                ofs.open(file_path, ios::trunc);
            }

            ofs << file_chunk.contents();

        }
        ofs.close();

        struct stat file_info;

        if (stat(file_path.c_str(), &file_info) != 0){
            return Status(StatusCode::NOT_FOUND, strerror(errno));
        }

        FileStatus file_status;
        response->set_file_name(fileName);
        Timestamp* modified = new Timestamp(file_status.modified());
        response->set_allocated_modified(modified);
        return Status::OK;
    }

    Status FetchFile(
        ServerContext* context, 
        const File* request,
        ServerWriter<FileStream>* writer
    ) override {
        const string& file_path = WrapPath(request->file_name());
        struct stat file_info;

        stat(file_path.c_str(), &file_info);

        if (stat(file_path.c_str(), &file_info) != 0){

            return Status(StatusCode::NOT_FOUND, "file not found");
        }

        int file_len = file_info.st_size;
        
        ifstream ifs(file_path);
        FileStream file_chunk;

        int bytes_sent = 0;

        while(bytes_sent < file_len){

            char buff[10240];

            int bytes_remaining = file_len - bytes_sent;
            if(10240 < bytes_remaining){
                bytes_remaining = 10240;
            }

            ifs.read(buff, bytes_remaining);
            file_chunk.set_contents(buff, bytes_remaining);
            writer->Write(file_chunk);
            bytes_sent = bytes_sent + bytes_remaining;
        }

        ifs.close();

        return Status::OK;
    }

    Status DeleteFile(
        ServerContext* context, 
        const File* request,
        ServerResponse* response
    ) override {
        const string& file_path = WrapPath(request->file_name());

        struct stat file_info;
        if (stat(file_path.c_str(), &file_info) != 0){
            return Status(StatusCode::NOT_FOUND, "file not found");
        }

        remove(file_path.c_str());

        FileStatus file_status;

        response->set_file_name(request->file_name());
        Timestamp* modified = new Timestamp(file_status.modified());
        response->set_allocated_modified(modified);
        return Status::OK;
    }

    Status ListFiles(
        ServerContext* context,
        const VoidArg* request,
        Files* response
    ) override {

        DIR *dir;

        if ((dir = opendir(mount_path.c_str())) == NULL) {
            return Status::OK;
        }

        //a structure type used to return information about directory entries
        struct dirent *entry;

        //The readdir() function returns a pointer to a structure representing the directory entry
        // at the current position in the directory stream specified by the argument dirp,
        // and positions the directory stream at the next entry.
        // It returns a null pointer upon reaching the end of the directory stream.
        while ((entry = readdir(dir)) != NULL) {

            //prepare buff path_info to see if the entry is a directory
            struct stat path_info;

            //string dir_name(entry->d_name);

            string file_path = WrapPath(entry->d_name);

            //obtain information about the named file
            //and write it to the area pointed to by the buf argument.
            //stat(file_path.c_str(), &path_info);
            //skip if file_path is a directory

            if(stat(file_path.c_str(), &path_info) == 0){

                if(path_info.st_mode & S_IFREG){
                    ServerResponse* file = response->add_file();
                    FileStatus file_status;

                    struct stat file_info;
                    if (stat(file_path.c_str(), &file_info) != 0){
                        return Status(StatusCode::NOT_FOUND, "file not found");
                    }

                    file_status.set_size(file_info.st_size);
                    file_status.set_file_name(file_path);

                    Timestamp* modified = new Timestamp(TimeUtil::TimeTToTimestamp(file_info.st_mtime));
                    Timestamp* created = new Timestamp(TimeUtil::TimeTToTimestamp(file_info.st_ctime));
                    file_status.set_allocated_modified(modified);
                    file_status.set_allocated_created(created);

                    file->set_file_name(entry->d_name);
                    modified = new Timestamp(file_status.modified());
                    file->set_allocated_modified(modified);

                }
            }


        }
        closedir(dir);
        return Status::OK;
    }

    Status GetStatus(
        ServerContext* context,
        const File* request,
        FileStatus* response
    ) override {

        string file_path = WrapPath(request->file_name());

        struct stat file_info;
        if (stat(file_path.c_str(), &file_info) != 0){
            return Status(StatusCode::NOT_FOUND, "file not found");
        }

        return Status::OK;
    }

};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly,
// but be aware that the testing environment is expecting these three
// methods as-is.
//
/**
 * The main server node constructor
 *
 * @param server_address
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        std::function<void()> callback) :
    server_address(server_address), mount_path(mount_path), grader_callback(callback) {}

/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
    this->server->Shutdown();
}

/** Server start **/
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path);
    ServerBuilder builder;
    builder.AddListeningPort(this->server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    this->server = builder.BuildAndStart();
    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    this->server->Wait();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional DFSServerNode definitions here
//

