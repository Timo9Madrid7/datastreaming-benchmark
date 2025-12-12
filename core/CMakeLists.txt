add_library(core-utils STATIC) # archive of object files for use when linking other targets
target_sources(core-utils PRIVATE # add more source files as needed
    utils/Env.cpp
)
target_include_directories(core-utils PUBLIC # make the utils reachable for other targets
    ${CMAKE_CURRENT_SOURCE_DIR}/utils 
)
set_target_properties(core-utils PROPERTIES POSITION_INDEPENDENT_CODE ON) # make it possible to link this static library to shared libraries

add_library(core-logger STATIC) # archive of object files for use when linking other targets
target_sources(core-logger PRIVATE 
        logger/Logger.cpp # this is required by other targets
)
find_package(spdlog REQUIRED)
target_include_directories(core-logger PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/logger # make the contents of this location readable for targets that link the library
)
target_link_libraries(core-logger PRIVATE spdlog::spdlog) # link spdlog to core-logger
set_target_properties(core-logger PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_library(core-loader STATIC)
target_sources(core-loader PRIVATE 
    technology_loader/TechnologyLoader.cpp # this is required by the apps
)
target_include_directories(core-loader PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/technology_loader # make the loader reachable for the apps
)
target_link_libraries(core-loader PUBLIC 
    core-logger
)
# if (WIN32)
#     target_link_libraries(core-loader PUBLIC # does it make sense to link this way? Interface linked to static?
#         core-logger
#     )
#     # in windows dynamic loading is solved differently, look it up another time
if(UNIX)
    target_link_libraries(core-loader PUBLIC
        dl
    )
endif()

add_library(core-payload STATIC)
target_sources(core-payload PRIVATE 
    payload/Payload.cpp
)
target_include_directories(core-payload PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/payload 
)
set_target_properties(core-payload PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_library(core-interfaces INTERFACE) # don't compile and don't produce library artifact on disk, but specify usage requirements
target_include_directories(core-interfaces INTERFACE 
    ${CMAKE_CURRENT_SOURCE_DIR}/interfaces # when linking this library, include these headers
)
target_link_libraries(core-interfaces INTERFACE
    core-logger
    core-payload
)

# Make this library available for use in other projects
# set_target_properties(core-logger PROPERTIES EXPORT_NAME Core-logger POSITION_INDEPENDENT_CODE ON)

add_library(factories SHARED) # Should be able to be static
target_sources(factories PUBLIC
    factory/PublisherFactory.cpp 
    factory/ConsumerFactory.cpp
)
target_include_directories(factories PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/factory # Make the factories reachable for the apps and the tech-registrations
)
target_link_libraries(factories PUBLIC core-interfaces)
# set_target_properties(factories PROPERTIES EXPORT_NAME Factories POSITION_INDEPENDENT_CODE ON)

