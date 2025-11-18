add_library(zeromq_p2p_technology SHARED)

target_sources(zeromq_p2p_technology PRIVATE
    ZeroMQP2PConsumer.cpp
    ZeroMQP2PPublisher.cpp
    ZeroMQP2PRegistration.cpp
)

set(ZMQ_INCLUDE_DIR "")
set(ZMQ_LIB_DIR "")

if(WIN32)
    # Use vcpkg paths on Windows
    set(ZMQ_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/../vcpkg/installed/x64-windows/include")
    set(ZMQ_LIB_DIR "${CMAKE_SOURCE_DIR}/../vcpkg/installed/x64-windows/lib")
    set(ZMQ_LIBRARY "${ZMQ_LIB_DIR}/libzmq-mt-4_3_5.lib")
elseif(UNIX)
    # Use system paths on Ubuntu/Docker
    set(ZMQ_INCLUDE_DIR "/usr/include")
    set(ZMQ_LIB_DIR "/usr/lib/x86_64-linux-gnu")
    set(ZMQ_LIBRARY "${ZMQ_LIB_DIR}/libzmq.so")
endif()


target_include_directories(zeromq_p2p_technology PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${ZMQ_INCLUDE_DIR}
)

target_link_libraries(zeromq_p2p_technology PRIVATE
    core-interfaces
    factories
    ${ZMQ_LIBRARY}
)
set_target_properties(zeromq_p2p_technology PROPERTIES OUTPUT_NAME "zeromq_p2p_technology")