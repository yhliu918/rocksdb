add_executable(index_block_test index_block_test.cc)
target_link_libraries(index_block_test ${ROCKSDB_LIB} ${THIRDPARTY_LIBS})
add_executable(filter_experiment filter_experiment.cc)
target_link_libraries(filter_experiment ${ROCKSDB_LIB} ${THIRDPARTY_LIBS})