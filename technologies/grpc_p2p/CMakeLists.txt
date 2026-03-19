add_library(grpc_p2p_technology SHARED)

target_sources(grpc_p2p_technology PRIVATE
    GrpcConsumer.cpp
    GrpcPublisher.cpp
    GrpcRegistration.cpp
)

# Find gRpc++ and protobuf
find_package(Protobuf CONFIG REQUIRED)

# Keep package roots consistent to avoid loading gRPC and Abseil from different installations.
if(DEFINED Protobuf_DIR)
    get_filename_component(_cmake_pkg_root "${Protobuf_DIR}" DIRECTORY)
    if(EXISTS "${_cmake_pkg_root}/grpc")
        set(gRPC_DIR "${_cmake_pkg_root}/grpc" CACHE PATH "gRPC package config directory" FORCE)
    endif()
    if(EXISTS "${_cmake_pkg_root}/absl")
        set(absl_DIR "${_cmake_pkg_root}/absl" CACHE PATH "Abseil package config directory" FORCE)
    endif()
endif()

find_package(gRPC CONFIG REQUIRED)
find_package(absl CONFIG REQUIRED)

set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)

# In WSL/Linux builds, prefer native binaries instead of Windows .exe tools.
if(UNIX)
    find_program(_PROTOBUF_PROTOC_NATIVE NAMES protoc)
    if(_PROTOBUF_PROTOC_NATIVE)
        set(_PROTOBUF_PROTOC "${_PROTOBUF_PROTOC_NATIVE}")
    endif()

    find_program(_GRPC_CPP_PLUGIN_NATIVE NAMES grpc_cpp_plugin)
    if(_GRPC_CPP_PLUGIN_NATIVE)
        set(_GRPC_CPP_PLUGIN_EXECUTABLE "${_GRPC_CPP_PLUGIN_NATIVE}")
    endif()
endif()

set(PROTO_SRC "${CMAKE_CURRENT_BINARY_DIR}/streaming.pb.cc")
set(PROTO_HEADER "${CMAKE_CURRENT_BINARY_DIR}/streaming.pb.h")
set(GRPC_SRC "${CMAKE_CURRENT_BINARY_DIR}/streaming.grpc.pb.cc")
set(GRPC_HEADER "${CMAKE_CURRENT_BINARY_DIR}/streaming.grpc.pb.h")

add_custom_command(
      OUTPUT ${PROTO_SRC} ${PROTO_HEADER} ${GRPC_SRC} ${GRPC_HEADER}
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out=${CMAKE_CURRENT_BINARY_DIR}
           --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
           --plugin=protoc-gen-grpc=${_GRPC_CPP_PLUGIN_EXECUTABLE}
           -I ${CMAKE_CURRENT_SOURCE_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/streaming.proto
      DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/streaming.proto
)

add_custom_target(generate_grpc_protos 
    DEPENDS ${PROTO_SRC} ${PROTO_HEADER} ${GRPC_SRC} ${GRPC_HEADER}
)

target_sources(grpc_p2p_technology PRIVATE
    ${PROTO_SRC}
    ${GRPC_SRC}
)

target_include_directories(grpc_p2p_technology PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_SOURCE_DIR}/third_party_libs/spsc_queue/
    ${CMAKE_SOURCE_DIR}/third_party_libs/thread_pool/
)

target_link_libraries(grpc_p2p_technology PRIVATE
    factories
    gRPC::grpc++
    protobuf::libprotobuf
)

set_target_properties(grpc_p2p_technology PROPERTIES OUTPUT_NAME "grpc_p2p_technology")