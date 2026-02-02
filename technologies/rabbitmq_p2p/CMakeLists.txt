add_library(rabbitmq_p2p_technology SHARED)

target_sources(rabbitmq_p2p_technology PRIVATE
    RabbitMQPublisher.cpp
    RabbitMQConsumer.cpp
    RabbitMQRegistration.cpp
)

# Find libevent and amqpcpp
find_path(EVENT_INCLUDE_DIR event2/event.h)
find_library(EVENT_LIBRARY event REQUIRED)
find_library(EVENT_PTHREADS_LIBRARY event_pthreads)

find_path(AMQP_INCLUDE_DIR amqpcpp.h)
find_library(AMQP_LIBRARY amqpcpp REQUIRED)

target_include_directories(rabbitmq_p2p_technology PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${EVENT_INCLUDE_DIR}
    ${AMQP_INCLUDE_DIR}
    ${CMAKE_SOURCE_DIR}/third_party_libs/spsc_queue/
)

target_link_libraries(rabbitmq_p2p_technology PRIVATE
    factories
    ${EVENT_LIBRARY}
    ${EVENT_PTHREADS_LIBRARY}
    ${AMQP_LIBRARY}
)

set_target_properties(rabbitmq_p2p_technology PROPERTIES OUTPUT_NAME "rabbitmq_p2p_technology")