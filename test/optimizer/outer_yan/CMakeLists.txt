add_library_unity(test_optimizer_outer_yan OBJECT
                  tree_conversions_roundtrip.cpp)

set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_optimizer_outer_yan>
    PARENT_SCOPE)
