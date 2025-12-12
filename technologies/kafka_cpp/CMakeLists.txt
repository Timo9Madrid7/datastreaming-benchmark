add_library(kafka_cpp_technology SHARED)

target_sources(kafka_cpp_technology PRIVATE
    KafkaCallbacks.cpp
    KafkaCppPublisher.cpp
    KafkaCppConsumer.cpp   
    KafkaCppRegistration.cpp
)

# Find librdkafka and lz4
find_path(RDKAFKA_INCLUDE_DIR librdkafka/rdkafkacpp.h)
find_library(RDKAFKA_CPP_LIBRARY rdkafka++ REQUIRED)

find_path(LZ4_INCLUDE_DIR lz4.h)
find_library(LZ4_LIBRARY lz4 REQUIRED)

target_include_directories(kafka_cpp_technology PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${RDKAFKA_INCLUDE_DIR}
    ${LZ4_INCLUDE_DIR}
)

target_link_libraries(kafka_cpp_technology PRIVATE
    factories
    ${RDKAFKA_CPP_LIBRARY}
    ${LZ4_LIBRARY}
)

set_target_properties(kafka_cpp_technology PROPERTIES OUTPUT_NAME "kafka_cpp_technology")