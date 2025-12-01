add_library(kafka_technology SHARED)

target_sources(kafka_technology PRIVATE
    KafkaPublisher.cpp
    KafkaConsumer.cpp   
    KafkaRegistration.cpp
)

set(KAFKA_INCLUDE_DIR "")
set(KAFKA_LIB_DIR "")

# First, check vcpkg path on Windows
if(WIN32)
    set(KAFKA_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/../vcpkg/installed/x64-windows-static/include")
    set(KAFKA_LIB_DIR "${CMAKE_SOURCE_DIR}/../vcpkg/installed/x64-windows-static/lib")
elseif(UNIX)
    set(KAFKA_INCLUDE_DIR "/usr/include")
    set(KAFKA_LIB_DIR "/usr/lib/x86_64-linux-gnu")
endif()

# Use find_library to locate required .lib or .so files
find_library(RDKAFKA_LIBRARY NAMES rdkafka REQUIRED)
find_library(LZ4_LIBRARY NAMES lz4 REQUIRED)

# Add include dirs
target_include_directories(kafka_technology PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${KAFKA_INCLUDE_DIR}
)

# Link the shared libraries
target_link_libraries(kafka_technology PRIVATE
    factories
    ${RDKAFKA_LIBRARY}
    ${LZ4_LIBRARY}
)

set_target_properties(kafka_technology PROPERTIES OUTPUT_NAME "kafka_technology")