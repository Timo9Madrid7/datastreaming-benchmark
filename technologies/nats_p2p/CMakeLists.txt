add_library(nats_p2p_technology SHARED)

target_sources(nats_p2p_technology PRIVATE
    NatsConsumer.cpp
    NatsPublisher.cpp
    NatsRegistration.cpp
)

# Find nats c client (header + library) via find_path/find_library
find_path(NATS_INCLUDE_DIR nats/nats.h)
find_library(NATS_LIBRARY nats REQUIRED)

target_include_directories(nats_p2p_technology PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${NATS_INCLUDE_DIR}
)

target_link_libraries(nats_p2p_technology PRIVATE
    factories
    ${NATS_LIBRARY}
)

set_target_properties(nats_p2p_technology PROPERTIES OUTPUT_NAME "nats_p2p_technology")