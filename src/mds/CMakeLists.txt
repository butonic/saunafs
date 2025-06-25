include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_definitions(-DMDS)
add_definitions(-DAPPNAME=sfsmds -DAPP_EXAMPLES_SUBDIR="${SFSMDS_EXAMPLES_SUBDIR}")

collect_sources(MDS)

add_library(mds ${MDS_SOURCES})
target_link_libraries(mds master fdb)

create_unittest(mds ${MDS_TESTS})

add_executable(sfsmds ${MAIN_SRC})
target_link_libraries(sfsmds mds ${PAM_LIBRARIES})
if(SYSTEMD_FOUND)
  target_link_libraries(sfsmds ${SYSTEMD_LIBRARIES})
endif()
install(TARGETS sfsmds RUNTIME DESTINATION ${SBIN_SUBDIR})
