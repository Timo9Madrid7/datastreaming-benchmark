add_executable(ConsumerApp ConsumerApp.cpp)
target_link_libraries(ConsumerApp PRIVATE core-loader factories)


add_executable(PublisherApp PublisherApp.cpp)
target_link_libraries(PublisherApp PRIVATE core-loader factories)