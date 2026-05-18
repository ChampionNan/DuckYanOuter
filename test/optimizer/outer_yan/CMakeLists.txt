add_library_unity(test_optimizer_outer_yan OBJECT
                  tree_conversions_roundtrip.cpp
                  semi_join_filter_test.cpp
                  key_bitmap_test.cpp
                  hash_filter_test.cpp
                  applicability_test.cpp
                  passes_integration_test.cpp)

set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_optimizer_outer_yan>
    PARENT_SCOPE)
