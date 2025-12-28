add_library(arrow_flight_technology SHARED)

target_sources(arrow_flight_technology PRIVATE
    ArrowFlightConsumer.cpp
    ArrowFlightPublisher.cpp
    ArrowFlightRegistration.cpp
)

# Find Arrow + Arrow Flight
find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED)

target_include_directories(arrow_flight_technology PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(arrow_flight_technology PRIVATE
    factories
    Arrow::arrow_shared 
    ArrowFlight::arrow_flight_shared
)

set_target_properties(arrow_flight_technology PROPERTIES OUTPUT_NAME "arrow_flight_technology")