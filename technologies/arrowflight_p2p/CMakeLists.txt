add_library(arrowflight_p2p_technology SHARED)

target_sources(arrowflight_p2p_technology PRIVATE
    ArrowFlightConsumer.cpp
    ArrowFlightPublisher.cpp
    ArrowFlightRegistration.cpp
)

# Find Arrow + Arrow Flight
find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED)

target_include_directories(arrowflight_p2p_technology PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/third_party_libs/
)

target_link_libraries(arrowflight_p2p_technology PRIVATE
    factories
    Arrow::arrow_shared 
    ArrowFlight::arrow_flight_shared
)

set_target_properties(arrowflight_p2p_technology PROPERTIES OUTPUT_NAME "arrowflight_p2p_technology")