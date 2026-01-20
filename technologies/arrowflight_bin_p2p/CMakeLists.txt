add_library(arrowflight_bin_p2p_technology SHARED)

target_sources(arrowflight_bin_p2p_technology PRIVATE
    ArrowFlightConsumer.cpp
    ArrowFlightPublisher.cpp
    ArrowFlightRegistration.cpp
)

# Find Arrow + Arrow Flight
find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED)

target_include_directories(arrowflight_bin_p2p_technology PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/third_party_libs/thread_pool/
)

target_link_libraries(arrowflight_bin_p2p_technology PRIVATE
    factories
    Arrow::arrow_shared 
    ArrowFlight::arrow_flight_shared
)

set_target_properties(arrowflight_bin_p2p_technology PROPERTIES OUTPUT_NAME "arrowflight_bin_p2p_technology")