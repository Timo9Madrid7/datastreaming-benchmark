add_library(nats_jetstream_p2p_technology SHARED)

target_sources(nats_jetstream_p2p_technology PRIVATE
    NatsJetStreamConsumer.cpp
    NatsJetStreamPublisher.cpp
    NatsJetStreamRegistration.cpp
)

# Find nats c client (header + library) via find_path/find_library
find_path(NATS_INCLUDE_DIR nats/nats.h)
find_library(NATS_LIBRARY nats REQUIRED)

set(NATS_ALL_INCLUDES
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${NATS_INCLUDE_DIR}
    ${CMAKE_SOURCE_DIR}/third_party_libs/spsc_queue/
)

target_include_directories(nats_jetstream_p2p_technology PRIVATE
    ${NATS_ALL_INCLUDES}
)

target_link_libraries(nats_jetstream_p2p_technology PRIVATE
    factories
    ${NATS_LIBRARY}
)

set_target_properties(nats_jetstream_p2p_technology PROPERTIES OUTPUT_NAME "nats_jetstream_p2p_technology")
